package krati.cds.impl.array;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import krati.cds.impl.segment.Segment;
import krati.cds.impl.segment.SegmentManager;

/**
 * SimpleDataArray Compactor.
 * 
 * The compaction is a two-stage process: inspect and compact.
 * 
 * The "inspect" determines which segments are eligible for compaction according to
 * a predefined load factor. It returns no more than 3 source segments for compaction.
 * 
 * The "compact" transfers bytes from source segments to a target segment via zero-copy.
 * It batches compaction update records and sends them to the writer for post-processing. 
 * 
 * @author jwu
 *
 */
class SimpleDataArrayCompactor implements Runnable
{
    private final static Logger _log = Logger.getLogger(SimpleDataArrayCompactor.class);
    
    private final SimpleDataArray _dataArray;
    private volatile double _compactLoadFactor;
    private volatile State _state = State.DONE;
    
    /**
     * Reclaim segments in _segSourceList and transfer their content to _segTarget.
     */
    private Segment _segTarget;
    private long _segTargetAppendPosition;
    private final ArrayList<Segment> _segSourceList;
    
    /**
     * Manage compaction updates that will be consumed by the writer.
     */
    private final CompactionUpdateManager _updateManager;
    
    /**
     * Lock for synchronizing compactor executions.
     */
    private final ReentrantLock _lock = new ReentrantLock();
    
    /**
     * Constructs a DataArrayCompactor with the setting below:
     * 
     * <pre>
     *   Compact Load Factor : 0.5
     *   Compact batch Size  : 1000
     * </pre>
     * 
     * A segment is eligible for compaction only if its load factor is less than
     * the default compact load factor (0.5).
     * 
     * @param dataArray          the data array to compact
     */
    public SimpleDataArrayCompactor(SimpleDataArray dataArray)
    {
        this(dataArray, 0.5, 1000);
    }
    
    /**
     * Constructs a DataArrayCompactor with a specified compact load factor and a default compact batch size (1000).
     * 
     * A segment is eligible for compaction only if its load factor is less than
     * the user-specified compact load factor.
     * 
     * @param dataArray          the data array to compact
     * @param compactLoadFactor  the load factor below which a segment is eligible for compaction
     */
    public SimpleDataArrayCompactor(SimpleDataArray dataArray, double compactLoadFactor)
    {
        this(dataArray, compactLoadFactor, 1000);
    }
    
    /**
     * Constructs a DataArrayCompactor with a specified compact load factor and compact batch size.
     * 
     * A segment is eligible for compaction only if its load factor is less than
     * the user-specified compact load factor.
     * 
     * @param dataArray          the data array to compact
     * @param compactLoadFactor  the load factor below which a segment is eligible for compaction
     * @param compactBatchSize   the size of compaction updates delivered by the compactor to the writer. 
     */
    public SimpleDataArrayCompactor(SimpleDataArray dataArray, double compactLoadFactor, int compactBatchSize)
    {
        this._dataArray = dataArray;
        this._compactLoadFactor = compactLoadFactor;
        this._segSourceList = new ArrayList<Segment>();
        this._updateManager = new CompactionUpdateManager(_dataArray, compactBatchSize);
    }
    
    public double getCompactLoadFactor()
    {
        return this._compactLoadFactor;
    }
    
    private static Comparator<Segment> _segmentLoadCmp = new Comparator<Segment>()
    {
        @Override
        public int compare(Segment s1, Segment s2)
        {
            double load1 = s1.getLoadSize();
            double load2 = s2.getLoadSize();
            return (load1 < load2) ? -1 : ((load1 == load2) ? 0 : 1);
        }
    };
    
    private boolean inspect() throws IOException
    {
        SegmentManager segManager = _dataArray.getSegmentManager();
        Segment segCurrent = _dataArray.getCurrentSegment();
        if(segManager == null) return false;
        
        synchronized(segManager)
        {
            /*
             * Find source segments that are least loaded.
             * The source segments must be in the READ_ONLY mode.
             */
            ArrayList<Segment> recycleList = new ArrayList<Segment>();
            int cnt = segManager.getSegmentCount();
            for(int i = 0; i < cnt; i++)
            {
                Segment seg = segManager.getSegment(i);
                if(seg != null && seg.getMode() == Segment.Mode.READ_ONLY && seg != segCurrent)
                {
                    if (seg.getLoadFactor() < _compactLoadFactor)
                    {
                        recycleList.add(seg);
                    }
                }
            }
            
            // No segment need compaction
            if (recycleList.size() == 0) return false;
            
            // Sort recycleList in ascending order of load size
            Collections.sort(recycleList, _segmentLoadCmp);
            
            // Compact no more than 3 segments per compaction cycle.
            // The total of segment load factors need to be less than
            // 0.8 to allow 20% inaccuracy (for safety).
            double totalFactor = 0;
            for(int i = 0, len = Math.min(3, recycleList.size()); i < len; i++)
            {
                Segment seg = recycleList.get(i);
                if(totalFactor < 0.8)
                {
                    totalFactor += seg.getLoadFactor();
                    if(totalFactor < 0.8)
                    {
                        _segSourceList.add(seg);
                    }
                }
                else
                {
                    break;
                }
            }
            
            // Delay compaction if only one segment is eligible for compaction but it is not VERY fragmented.
            if (_segSourceList.size() == 1 && _segSourceList.get(0).getLoadFactor() > (_compactLoadFactor/2)) return false;
            
            for(Segment seg : _segSourceList)
            {
                _log.info("Segment " + seg.getSegmentId() + " load factor=" + ((long)(seg.getLoadFactor() * 10000) / 10000.0));
            }
            
            _log.info("inspect done");
            return true;
        }
        
        /*
         * No synchronization on segManager is required after locating segments eligible for compaction.
         */
    }
    
    private boolean compact() throws IOException
    {
        try
        {
            _segTarget = _dataArray.getSegmentManager().nextSegment();
            _segTargetAppendPosition = _segTarget.getAppendPosition();
            
            /* NOTE:
             * From here, getAppendPosition() is not reliable for the compactor.
             * This is because the writer updates appendPosition asynchronously.
             */
            
            for(Segment seg : _segSourceList)
            {
                if(compact(seg, _segTarget))
                {
                    _dataArray._compactedSegmentQueue.add(seg);
                }
                else
                {
                    break;
                }
            }
            
            _log.info("bytes transferred to   " + _segTarget.getSegmentId() + ": " + (_segTargetAppendPosition  - Segment.dataStartPosition));
        }
        catch(Exception e)
        {
            _log.warn(e.getMessage(), e);
            return false;
        }
        
        _log.info("compact done");
        return true;
    }
    
    private boolean compact(Segment segSource, Segment segTarget) throws IOException
    {
        RandomAccessFile raf;
        int segSourceId = segSource.getSegmentId();
        int segTargetId = segTarget.getSegmentId();
        
        // Get write channel
        raf = new RandomAccessFile(segTarget.getSegmentFile(), "rw");
        FileChannel writeChannel = raf.getChannel();
        
        // Cannot use segTarget.getAppendPosition() as the writer and compactor work asynchronously.
        // Position write channel properly
        writeChannel.position(_segTargetAppendPosition);
        
        long sizeLimit = segTarget.getInitialSize();
        long bytesTransferred = 0;
        boolean succ = true;
        
        try
        {
            int offsetMask = _dataArray._offsetMask;
            int segmentMask = _dataArray._segmentMask;
            int segmentShift = _dataArray._segmentShift;
            
            for(int index = 0, cnt = _dataArray.length(); index < cnt; index++)
            {
                long oldAddress = _dataArray.getAddress(index);
                int oldSegPos = (int)(oldAddress & offsetMask);
                int oldSegInd = (int)((oldAddress >> segmentShift) & segmentMask);
                
                if (oldSegInd == segSourceId && oldSegPos >= Segment.dataStartPosition)
                {
                    int length = segSource.readInt(oldSegPos);
                    int byteCnt = 4 + length;
                    long newSegPos = writeChannel.position();
                    long newAddress = (((long)segTargetId) << segmentShift) | newSegPos;
                    
                    if(writeChannel.position() + byteCnt >= sizeLimit)
                    {
                        succ = false;
                        break;
                    }
                    
                    // Transfer bytes from source to target
                    segSource.transferTo(oldSegPos, byteCnt, writeChannel);
                    bytesTransferred += byteCnt;
                    
                    _updateManager.addUpdate(index, byteCnt, newAddress, segTarget, writeChannel);
                }
            }
            
            // Push whatever left into update queue
            _segTargetAppendPosition += bytesTransferred;
            _updateManager.endUpdate(segTarget, writeChannel);
            _log.info("bytes transferred from " + segSource.getSegmentId() + ": " + bytesTransferred);
            
            // Close write channel
            writeChannel.force(true);
            writeChannel.close();
            writeChannel = null;
            
            return succ;
        }
        finally
        {
            if(writeChannel != null) writeChannel.close();
        }
    }
    
    @Override
    public void run()
    {
        // One and only one compactor is at work.
        _lock.lock();
        
        try
        {
            reset();
            _state = State.INIT;
            _log.info("compaction started");
            
            // Inspect the array
            if(!inspect()) return;
            
            // Compact the array
            if(!compact()) return;
        }
        catch(Exception e)
        {
            e.printStackTrace(System.err);
            _log.error("failed to compact: " + e.getMessage());
        }
        finally
        {
            reset();
            _state = State.DONE;
            _log.info("compaction ended");
            _lock.unlock();
        }
    }
    
    public void start() throws InterruptedException
    {
        _state = State.INIT;
        new Thread(this).start();
    }
    
    public boolean isStarted()
    {
        return _state != State.DONE;
    }
    
    protected void reset()
    {
        _segTarget = null;
        _segSourceList.clear();
        _updateManager.clear();
    }
    
    protected void lock()
    {
        _lock.lock();
    }
    
    protected void unlock()
    {
        _lock.unlock();
    }
    
    protected CompactionUpdateBatch pollCompactionBatch()
    {
        return _updateManager.pollBatch();
    }
    
    protected boolean recycleCompactionBatch(CompactionUpdateBatch batch)
    {
        return _updateManager.recycleBatch(batch);
    }
    
    static enum State {
        INIT,
        DONE;
    }
    
    static class CompactionUpdate
    {
        int _index;
        int _dataSize;
        long _address;
        
        CompactionUpdate(int index, int dataSize, long address)
        {
            this._index = index;
            this._address = address;
            this._dataSize = dataSize;
        }
        
        public String toString()
        {
            StringBuffer buf = new StringBuffer();
            
            buf.append(getClass().getSimpleName());
            buf.append("{_index=");
            buf.append(_index);
            buf.append(", _dataSize=");
            buf.append(_dataSize);
            buf.append(", _address=");
            buf.append(_address);
            buf.append("}");
            
            return buf.toString();
        }
    }
    
    static class CompactionUpdateBatch
    {
        static int _counter = 0;
        final int _batchId;
        final int _capacity;
        final int _unitSize = 16;
        final ByteBuffer _buffer;
        
        Segment _segTarget = null;
        int _dataSizeTotal = 0;
        int _serviceId = 0;
        long _lwMark = 0;
        
        CompactionUpdateBatch(int capacity)
        {
            this._capacity = capacity;
            this._batchId = _counter++;
            this._buffer = ByteBuffer.allocate(_capacity * _unitSize);
            _log.info("CompactionUpdateBatch " + _batchId);
        }
        
        public void clear()
        {
            _buffer.clear();
            _segTarget = null;
            _dataSizeTotal = 0;
            _serviceId = 0;
            _lwMark = 0;
        }
        
        public int getCapacity()
        {
            return _capacity;
        }
        
        public int getByteCapacity()
        {
            return _buffer.capacity();
        }
        
        public ByteBuffer getInternalBuffer()
        {
            return _buffer;
        }
        
        public int size()
        {
            return _buffer.position()/_unitSize;
        }
        
        public boolean isEmpty()
        {
            return _buffer.position() == 0;
        }
        
        public int getBatchId()
        {
            return _batchId;
        }

        public int getServiceId()
        {
            return _serviceId;
        }
        
        public String getDescriptiveId()
        {
            return ((_segTarget == null) ? "?[" : (_segTarget.getSegmentId() + "[")) + _serviceId + "]";
        }
        
        public long getLWMark()
        {
            return _lwMark;
        }
        
        public Segment getTargetSegment()
        {
            return _segTarget;
        }
        
        public void add(int index, int dataSize, long address)
        {
            _buffer.putInt(index);
            _buffer.putInt(dataSize);
            _buffer.putLong(address);
            _dataSizeTotal += dataSize;
        }
        
        public CompactionUpdate get(int i)
        {
            return new CompactionUpdate(getUpdateIndex(i),
                                        getUpdateDataSize(i),
                                        getUpdateAddress(i));
        }
        
        public int getUpdateIndex(int i)
        {
            return _buffer.getInt(i << 4);
        }
        
        public int getUpdateDataSize(int i)
        {
            return _buffer.getInt((i << 4) + 4);
        }
        
        public long getUpdateAddress(int i)
        {
            return _buffer.getLong((i << 4) + 8);
        }
        
        public int getDataSizeTotal()
        {
            return _dataSizeTotal;
        }
        
        void setLWMark(long waterMark)
        {
            _lwMark = waterMark;
        }
        
        void setTargetSegment(Segment seg)
        {
            _segTarget = seg;
        }
        
        void setServiceId(int serviceId)
        {
            _serviceId = serviceId;
        }
    }
    
    static class CompactionUpdateManager
    {
        private final int _batchSize;
        private final ConcurrentLinkedQueue<CompactionUpdateBatch> _serviceBatchQueue;
        private final ConcurrentLinkedQueue<CompactionUpdateBatch> _recycleBatchQueue;
        private final SimpleDataArray _dataArray;
        private int _batchServiceIdCounter = 0; 
        private CompactionUpdateBatch _batch;
        
        public CompactionUpdateManager(SimpleDataArray dataArray, int batchSize)
        {
            _dataArray = dataArray;
            _batchSize = batchSize;
            _serviceBatchQueue = new ConcurrentLinkedQueue<CompactionUpdateBatch>();
            _recycleBatchQueue = new ConcurrentLinkedQueue<CompactionUpdateBatch>();
            nextBatch();
        }
        
        private void nextBatch()
        {
            _batch = _recycleBatchQueue.poll();
            if(_batch == null)
            {
                _batch = new CompactionUpdateBatch(_batchSize);
            }
            
            _batch.clear();
            _batch.setServiceId(_batchServiceIdCounter++);
        }
        
        public boolean isServiceQueueEmpty()
        {
            return _serviceBatchQueue.isEmpty();
        }

        public boolean isRecycleQueueEmpty()
        {
            return _recycleBatchQueue.isEmpty();
        }
        
        public CompactionUpdateBatch pollBatch()
        {
            return _serviceBatchQueue.poll();
        }

        public boolean recycleBatch(CompactionUpdateBatch batch)
        {
            batch.clear();
            return _recycleBatchQueue.add(batch);
        }
        
        public void addUpdate(int index, int dataSize, long address, Segment segTarget, FileChannel writeChannel) throws IOException
        {
            try
            {
                _batch.add(index, dataSize, address);
            }
            catch(BufferOverflowException e)
            {
                writeChannel.force(true);
                _batch.setTargetSegment(segTarget);
                _batch.setLWMark(_dataArray.getLWMark());
                _log.info("compaction batch " + _batch.getDescriptiveId() + " hwMark=" + _batch.getLWMark());
                
                _serviceBatchQueue.add(_batch);
                nextBatch();
                
                // Add compaction update to new batch
                _batch.add(index, dataSize, address);
            }
        }
        
        public void endUpdate(Segment segTarget, FileChannel writeChannel) throws IOException
        {
            writeChannel.force(true);
            _batch.setTargetSegment(segTarget);
            _batch.setLWMark(_dataArray.getLWMark());
            _log.info("compaction batch " + _batch.getDescriptiveId() + " hwMark=" + _batch.getLWMark());
            
            _serviceBatchQueue.add(_batch);
            _batchServiceIdCounter = 0;
            nextBatch();
        }
        
        public void clear()
        {
            _batchServiceIdCounter = 0;
            _batch.clear();
            _batch.setServiceId(_batchServiceIdCounter++);
        }
    }
}
