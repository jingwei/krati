package krati.cds.impl.array;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import org.apache.log4j.Logger;

import krati.cds.array.DataArray;
import krati.cds.array.LongArray;
import krati.cds.impl.array.basic.LongArrayMemoryImpl;
import krati.cds.impl.array.basic.LongArrayRecoverableImpl;
import krati.cds.impl.array.entry.Entry;
import krati.cds.impl.array.entry.EntryFileWriter;
import krati.cds.impl.array.entry.EntryPersistAdapter;
import krati.cds.impl.array.entry.EntryValue;
import krati.cds.impl.segment.AddressFormat;
import krati.cds.impl.segment.Segment;
import krati.cds.impl.segment.SegmentException;
import krati.cds.impl.segment.SegmentManager;
import krati.cds.impl.segment.SegmentOverflowException;

/**
 * DataArrayImpl: Simple Persistent DataArray Implementation.
 * 
 * This class is not thread-safe by design. It is expected that the conditions below hold within one JVM.
 * <pre>
 *    1. There is one and only one instance of DataArrayImpl for a given cacheDirectory.
 *    2. There is one and only one thread is calling setData methods at any given time. 
 * </pre>
 * 
 * It is expected that this class is used in the case of multiple readers and single writer.
 * 
 * @author jwu
 *
 */
public class DataArrayImpl implements DataArray
{
    private final static Logger _log = Logger.getLogger(DataArrayImpl.class);
    
    protected Segment _segment;              // current segment to append
    protected boolean _canTriggerCompaction; // current segment can trigger compaction only once
    
    protected volatile LongArray _addressArray;
    protected volatile SegmentManager _segmentManager;
    protected DataArrayImplCompactor _compactor;
    
    protected final double _segmentCompactFactor;
    protected final double _segmentCompactTrigger;
    protected long _segmentCompactTriggerLowerPosition;
    protected long _segmentCompactTriggerUpperPosition;
    
    protected final int _offsetMask;
    protected final int _segmentMask;
    protected final int _segmentShift;
    
    private long _metaUpdateOnAppendPosition = Segment.dataStartPosition;
    
    /**
     * Constructs a DataArray with Segment Compact Trigger default to 0.1 and Segment Compact Factor default to 0.5. 
     * 
     * @param addressArray           Array of addresses (longs) to positions of Segment.
     * @param segmentManager         Segment manager for loading, creating, freeing, maintaining segments.
     * 
     */
    public DataArrayImpl(LongArrayRecoverableImpl addressArray, SegmentManager segmentManager)
    {
        this(addressArray, segmentManager, 0.1, 0.5);
    }
    
    /**
     * Constructs a DataArray.
     * 
     * @param addressArray           Array of addresses (longs) to positions of Segment.
     * @param segmentManager         Segment manager for loading, creating, freeing, maintaining segments.
     * @param segmentCompactTrigger  Percentage of segment capacity, which writes trigger compaction once per segment.
     * @param segmentCompactFactor   Load factor below which a segment is eligible for compaction. Recommended value is 0.5.
     */
    public DataArrayImpl(LongArrayRecoverableImpl addressArray,
                         SegmentManager segmentManager,
                         double segmentCompactTrigger,
                         double segmentCompactFactor)
    {
        this._addressArray = addressArray;
        this._segmentManager = segmentManager;
        this._segmentCompactFactor = segmentCompactFactor;
        this._segmentCompactTrigger = segmentCompactTrigger;
        
        addressArray.getEntryManager().setEntryPersistListener(new SegmentPersistListener());
        
        AddressFormat f = new AddressFormat(16);
        this._segmentShift = f.getSegmentShift();
        this._segmentMask = f.getSegmentMask();
        this._offsetMask = f.getOffsetMask();
        
        this.init();

        this._compactor = new DataArrayImplCompactor(this, getSegmentCompactFactor());
        this._canTriggerCompaction = true;
        this.updateCompactTriggerBounds();
        
        this._metaUpdateOnAppendPosition = Segment.dataStartPosition;
    }
    
    /* ************************************************************************************************ *
     * The protected constructor and methods compact, catchup, and wrap are provided for the compactor. *
     * ************************************************************************************************ */
    
    protected DataArrayImpl(LongArrayMemoryImpl memAddressArray, SegmentManager segmentManager, List<Segment> segTargetList)
    {
        this._addressArray = memAddressArray;
        this._segmentManager = segmentManager;
        this._segmentCompactFactor = 0;        // No segment will be compacted.
        this._segmentCompactTrigger = 1;       // No compaction will be triggered.
        
        AddressFormat f = new AddressFormat(16);
        this._segmentShift = f.getSegmentShift();
        this._segmentMask = f.getSegmentMask();
        this._offsetMask = f.getOffsetMask();
        
        this.init();
        
        this._compactor = null;
        this._canTriggerCompaction = false;
        this.updateCompactTriggerBounds();
        
        this._metaUpdateOnAppendPosition = Segment.dataStartPosition;
        
        // Add current segment to segTargetList
        segTargetList.add(_segment);
    }
    
    protected long compact(Segment segSource,
                           List<Segment> segTargetList,
                           EntryFileWriter entryWriter) throws IOException
    {
        int segTargetCnt = 1;
        RandomAccessFile raf;
        Segment segTarget = null;
        
        if(segTargetList.size() > 0)
        {
            segTarget = segTargetList.get(segTargetList.size() - 1);
        }
        else
        {
            segTarget = getSegmentManager().nextSegment();
            segTargetList.add(segTarget);
        }
        
        int segSourceId = segSource.getSegmentId();
        int segTargetId = segTarget.getSegmentId();
        
        // Get read channel
        raf = new RandomAccessFile(segSource.getSegmentFile(), "r");
        FileChannel readChannel = raf.getChannel();
        
        // Get write channel
        raf = new RandomAccessFile(segTarget.getSegmentFile(), "rw");
        FileChannel writeChannel = raf.getChannel();
        
        // Position write channel properly
        writeChannel.position(segTarget.getAppendPosition());
        
        long sizeLimit = segTarget.getInitialSize();
        
        try
        {
            long bytesTransferred = 0;
            int indexStart = getIndexStart();
            long scn = entryWriter.getMinScn();
            
            for(int index = indexStart, cnt = length(); index < cnt; index++)
            {
                long oldAddress = getAddress(index);
                int oldSegPos = (int)(oldAddress & _offsetMask);
                int oldSegInd = (int)((oldAddress >> _segmentShift) & _segmentMask);
                
                if (oldSegInd == segSourceId && oldSegPos >= Segment.dataStartPosition)
                {
                    int length = segSource.readInt(oldSegPos);
                    int byteCnt = 4 + length;
                    long newSegPos = writeChannel.position();
                    long newAddress = (((long)segTargetId) << _segmentShift) | newSegPos;
                    
                    if(writeChannel.position() + byteCnt >= sizeLimit)
                    {
                        /* It should never require more than 2 segments.
                         * If it does happen, some kind of data corruption may have occurred.
                         * Should abort data transfer now and try compaction at another time.
                         */
                        if (segTargetCnt >= 2)
                        {
                            throw new CompactionAbortedException();
                        }
                        
                        // Update segTarget append position
                        writeChannel.force(true);
                        segTarget.setAppendPosition(writeChannel.position());
                        segTarget.asReadOnly();
                        writeChannel.close();
                        
                        String info = "transfer overflow: segment changed from " + segTarget.getSegmentId();
                        
                        // Get new segTarget
                        segTarget = getSegmentManager().nextSegment();
                        segTargetId = segTarget.getSegmentId();
                        segTargetList.add(segTarget);
                        segTargetCnt++;
                        
                        info = info + " to " + segTarget.getSegmentId();
                        
                        // Get new write channel
                        raf = new RandomAccessFile(segTarget.getSegmentFile(), "rw");
                        writeChannel = raf.getChannel();
                        
                        // Position write channel properly
                        writeChannel.position(segTarget.getAppendPosition());
                        
                        sizeLimit = segTarget.getInitialSize();
                        
                        _log.info(info);
                    }
                    
                    // Transfer byte from source to target
                    readChannel.transferTo(oldSegPos, byteCnt, writeChannel);
                    segTarget.incrLoadSize(byteCnt);
                    bytesTransferred += byteCnt;
                    
                    // Update address
                    try
                    {
                        setAddress(index, newAddress, 0);
                    }
                    catch(Exception e)
                    {
                        throw new IOException(e.getCause());
                    }
                    
                    // Log the original address into compactor entry file.
                    entryWriter.write(index - indexStart, oldAddress, scn);
                }
            }
            
            // Update segTarget append position
            segTarget.setAppendPosition(writeChannel.position());
            
            // Close read channel
            readChannel.close();
            readChannel = null;
            
            // Close write channel
            writeChannel.force(true);
            writeChannel.close();
            writeChannel = null;
            
            return bytesTransferred;
        }
        catch(IOException ioe)
        {
            // Update segTarget append position
            segTarget.setAppendPosition(writeChannel.position());
            
            if(readChannel != null) readChannel.close();
            if(writeChannel != null) writeChannel.close();
            throw ioe;
        }
    }
    
    protected void catchup(int index, long addressUpdate, long scnUpdate, List<Segment> segTargetList) throws Exception
    {
        long address = getAddress(index);
        int segPos = (int)(address & _offsetMask);
        int segInd = (int)((address >> _segmentShift) & _segmentMask);
        
        for(Segment segTarget: segTargetList)
        {
            if (segInd == segTarget.getSegmentId() && segPos >= Segment.dataStartPosition)
            {
                segTarget.decrLoadSize(segTarget.readInt(segPos));
                break;
            }
        }
        
        // Update address in the cloned memory array
        setAddress(index, addressUpdate, scnUpdate);
    }
    
    protected void wrap(LongArray memAddressArray, SegmentManager segmentManager) throws IOException
    {
        try
        {
            _segmentManager = segmentManager;
            ((LongArrayRecoverableImpl)_addressArray).wrap(memAddressArray);
            
            _log.info("wrapped array:" +
                      " indexStart=" + memAddressArray.getIndexStart() +
                      " length=" + memAddressArray.length());
        }
        catch(Exception e)
        {
            _log.error("failed to wrap array:"+
                       " indexStart=" + memAddressArray.getIndexStart() +
                       " length=" + memAddressArray.length());
            if(e instanceof IOException)
            {
                throw (IOException)e;
            }
            else
            {
                throw new IOException(e);
            }
        }
    }
    
    protected void init()
    {
        try
        {
            _segment = _segmentManager.nextSegment();
        }
        catch(IOException ioe)
        {
            _log.error(ioe.getMessage(), ioe);
            throw new SegmentException("Instantiation failed due to " + ioe.getMessage());
        }
    }
    
    protected void updateCompactTriggerBounds()
    {
        long size = getCurrentSegment().getInitialSize();
        long incr = (long)(size * 0.05);
        
        long compactTriggerLowerPosition = (long)(size * getSegmentCompactTrigger());
        _segmentCompactTriggerLowerPosition = compactTriggerLowerPosition - incr;
        _segmentCompactTriggerUpperPosition = compactTriggerLowerPosition + incr;
        
        if(_segmentCompactTriggerLowerPosition < Segment.dataStartPosition)
        {
            _segmentCompactTriggerLowerPosition = Segment.dataStartPosition;
        }
        
        if(_segmentCompactTriggerUpperPosition > size)
        {
            _segmentCompactTriggerUpperPosition = size;
        }
    }
    
    protected long getAddress(int index)
    {
        return _addressArray.getData(index);
    }
    
    protected void setAddress(int index, long value, long scn) throws Exception
    {
        _addressArray.setData(index, value, scn);
    }
    
    protected LongArray getAddressArray()
    {
        return _addressArray;
    }

    protected double getSegmentCompactFactor()
    {
        return _segmentCompactFactor;
    }
    
    protected double getSegmentCompactTrigger()
    {
        return _segmentCompactTrigger;
    }
    
    protected SegmentManager getSegmentManager()
    {
        return _segmentManager;
    }
    
    protected Segment getCurrentSegment()
    {
        return _segment;
    }
    
    protected void decrOriginalSegmentLoad(int index)
    {
        try
        {
            long address = getAddress(index);
            int segPos = (int)(address & _offsetMask);
            int segInd = (int)((address >> _segmentShift) & _segmentMask);
            
            if (segPos >= Segment.dataStartPosition)
            {
                // get data segment
                Segment seg = _segmentManager.getSegment(segInd);
                
                // read data length
                if(seg != null) seg.decrLoadSize(seg.readInt(segPos));
            }
        }
        catch(IOException e) {}
    }
    
    private void tryFlowControl()
    {
        Segment liveSegment = _segment;
        if(liveSegment == null)
        {
            return;
        }
        
        DataArrayImpl dataArrayCopy = _compactor.getDataArrayCopy();
        if(dataArrayCopy == null)
        {
            return;
        }
        
        SegmentManager segManagerCopy = dataArrayCopy.getSegmentManager();
        if(segManagerCopy == null)
        {
            return;
        }
        
        Segment compactSegment = segManagerCopy.getCurrentSegment();
        if(compactSegment == null || compactSegment == liveSegment)
        {
            return;
        }
        
        /*
         * Slow down the writer for 1 millisecond so that the compactor has a chance to catch up.
         */
        if(compactSegment.getLoadSize() < liveSegment.getLoadSize()) 
        {
            try
            {
                Thread.sleep(1);
            }
            catch(Exception e) {}
        }
    }
    
    @Override
    public int getDataLength(int index)
    {
        try
        {
            long address = getAddress(index);
            int segPos = (int)(address & _offsetMask);
            int segInd = (int)((address >> _segmentShift) & _segmentMask);
            
            // no data found
            if(segPos < Segment.dataStartPosition) return -1;
            
            // get data segment
            Segment seg = _segmentManager.getSegment(segInd);
            
            // read data length
            return seg.readInt(segPos);
        }
        catch(Exception e)
        {
            return -1;
        }
    }
    
    @Override
    public byte[] getData(int index)
    {
        try
        {
            long address = getAddress(index);
            int segPos = (int)(address & _offsetMask);
            int segInd = (int)((address >> _segmentShift) & _segmentMask);
            
            // no data found
            if(segPos < Segment.dataStartPosition) return null;
            
            // get data segment
            Segment seg = _segmentManager.getSegment(segInd);
            
            // read data length
            int len = seg.readInt(segPos);
            
            // read data into byte array
            byte[] data = new byte[len];
            if (len > 0)
            {
                seg.read(segPos + 4, data);
            }
            
            return data;
        }
        catch(Exception e)
        {
            return null;
        }
    }
    
    @Override
    public int getData(int index, byte[] data)
    {
        return getData(index, data, 0);
    }
    
    @Override
    public int getData(int index, byte[] data, int offset)
    {
        try
        {
            long address = getAddress(index);
            int segPos = (int)(address & _offsetMask);
            int segInd = (int)((address >> _segmentShift) & _segmentMask);
            
            // no data found
            if(segPos < Segment.dataStartPosition) return -1;
            
            // get data segment
            Segment seg = _segmentManager.getSegment(segInd);
            
            // read data length
            int len = seg.readInt(segPos);
            
            // read data into byte array
            if (len > 0)
            {
                seg.read(segPos + 4, data, offset, len);
            }
            
            return len;
        }
        catch(Exception e)
        {
            return -1;
        }
    }
    
    @Override
    public int transferTo(int index, WritableByteChannel channel)
    {
        try
        {
            long address = getAddress(index);
            int segPos = (int)(address & _offsetMask);
            int segInd = (int)((address >> _segmentShift) & _segmentMask);
            
            // no data found
            if(segPos < Segment.dataStartPosition) return -1;
            
            // get data segment
            Segment seg = _segmentManager.getSegment(segInd);
            
            // read data length
            int len = seg.readInt(segPos);
            
            // transfer data to a writable channel
            if (len > 0)
            {
                seg.transferTo(segPos + 4, len, channel);
            }
            
            return len;
        }
        catch(Exception e)
        {
            return -1;
        }
    }
    
    @Override
    public void setData(int index, byte[] data, long scn) throws Exception
    {
        if(data == null)
        {
            setData(index, data, 0, 0, scn);
        }
        else
        {
            setData(index, data, 0, data.length, scn);
        }
    }
    
    @Override
    public void setData(int index, byte[] data, int offset, int length, long scn) throws Exception
    {
        decrOriginalSegmentLoad(index);
        
        // no data
        if (data == null)
        {
            setAddress(index, 0, scn);
            return;
        }
        
        if (offset > data.length || (offset + length) > data.length)
        {
            throw new ArrayIndexOutOfBoundsException(data.length);
        }
        
        while(true)
        {
            // get append position
            long pos = _segment.getAppendPosition();
            
            try
            {
                // check append position is in range
                if ((pos >> _segmentShift) > 0)
                {
                    throw new SegmentOverflowException(_segment);
                }
                
                // append data size
                _segment.appendInt(length);
                
                // append actual data
                if (length > 0)
                {
                    _segment.append(data, offset, length);
                }
                
                // update addressArray 
                long address = (((long)_segment.getSegmentId()) << _segmentShift) | pos;
                setAddress(index, address, scn);
                
                // update segment meta on first write
                if (pos >= _metaUpdateOnAppendPosition)
                {
                    _segmentManager.updateMeta();
                    _metaUpdateOnAppendPosition = _segment.getInitialSize();
                }
                
                /* ******************************************************************************
                 * Trigger segment compaction (BEGIN)
                 */
                
                // current segment can trigger compaction once and only once
                if (_canTriggerCompaction &&
                    pos > _segmentCompactTriggerLowerPosition &&
                    pos < _segmentCompactTriggerUpperPosition )
                {
                    if(!_compactor.isStarted())
                    {
                        _log.info("Segment " + _segment.getSegmentId() + " triggered compaction");
                        
                        // persist this data array and apply entry log files 
                        persist();
                        
                        // disable auto-applying entry files
                        if (_addressArray instanceof LongArrayRecoverableImpl)
                        {
                            ((LongArrayRecoverableImpl)_addressArray).getEntryManager().setAutoApplyEntries(false);
                        }
                        
                        // start the compactor
                        _compactor.start();
                        _canTriggerCompaction = false;
                    }
                }
                
                // Notify the compactor of new address update
                if(_compactor.isStarted())
                {
                   _compactor.addressUpdated(index, address, scn);
                   
                   /*
                    * Flow-control the writer and average-out write latency.
                    * Give the compactor a chance to catch up with the writer.
                    */
                   tryFlowControl();
                }
                
                /*
                 * Trigger segment compaction (END)
                 * ******************************************************************************/
                
                return;
            }
            catch(SegmentOverflowException soe)
            {
                _log.info("Segment " + _segment.getSegmentId() + " filled");
                
                // update segment meta
                // _segmentManager.updateMeta();
                
                // wait until compactor is done
                while(_compactor.isStarted())
                {
                    _log.info("wait for compactor");
                    Thread.sleep(100);
                }
                
                synchronized(this) // synchronize the code below and the compactor
                {
                    // persist the current segment
                    persist();

                    // enable auto-applying entry files
                    if (_addressArray instanceof LongArrayRecoverableImpl)
                    {
                        ((LongArrayRecoverableImpl)_addressArray).getEntryManager().setAutoApplyEntries(true);
                    }
                    
                    // get the next segment available for appending
                    _metaUpdateOnAppendPosition = Segment.dataStartPosition;
                    _segment = _segmentManager.nextSegment(_segment);
                    _canTriggerCompaction = true;
                    
                    _log.info("Segment " + _segment.getSegmentId() + " live");
                }
            }
            catch(Exception e)
            {
                // restore append position 
                _segment.setAppendPosition(pos);
                
                // enable auto-applying entry files
                if (_addressArray instanceof LongArrayRecoverableImpl)
                {
                    ((LongArrayRecoverableImpl)_addressArray).getEntryManager().setAutoApplyEntries(true);
                }
                
                throw e;
            }
        }
    }
    
    @Override
    public int getIndexStart()
    {
        return _addressArray.getIndexStart();
    }
    
    @Override
    public boolean indexInRange(int index)
    {
        return _addressArray.indexInRange(index);
    }
    
    @Override
    public int length()
    {
        return _addressArray.length();
    }
    
    @Override
    public long getHWMark()
    {
        return _addressArray.getHWMark();
    }
    
    @Override
    public long getLWMark()
    {
        return _addressArray.getLWMark();
    }

    @Override
    public void saveHWMark(long endOfPeriod) throws Exception
    {
        _addressArray.saveHWMark(endOfPeriod);
    }
    
    @Override
    public synchronized void sync() throws IOException
    {
        /* The "persist" must be synchronized with data array compactor because
         * the compactor runs in a separate thread and will re-write the address
         * array file at the end of compaction.  
         */
        
        /* CALLS ORDERED:
         * Need force _segment first and then persist _addressArray.
         * During recovery, the _addressArray can always point to addresses
         * which are valid though may not reflect the most recent address update.
         */
        _segment.force();
        _addressArray.sync();
        _segmentManager.updateMeta();
    }
    
    @Override
    public synchronized void persist() throws IOException
    {
        /* The "persist" must be synchronized with data array compactor because
         * the compactor runs in a separate thread and will re-write the address
         * array file at the end of compaction.  
         */
        
        /* CALLS ORDERED:
         * Need force _segment first and then persist _addressArray.
         * During recovery, the _addressArray can always point to addresses
         * which are valid though may not reflect the most recent address update.
         */
        _segment.force();
        _addressArray.persist();
        _segmentManager.updateMeta();
    }
    
    @Override
    public synchronized void clear()
    {
        _addressArray.clear();
        _segmentManager.clear();
    }
    
    private class SegmentPersistListener extends EntryPersistAdapter
    {
        public void priorPersisting(Entry<? extends EntryValue> e) throws IOException
        {
            if(_segment != null)
            {
                _segment.force();
            }
        }
        
        public void afterPersisting(Entry<? extends EntryValue> e) throws IOException
        {
            if(_segmentManager != null)
            {
                _segmentManager.updateMeta();
            }
        }
    }
}
