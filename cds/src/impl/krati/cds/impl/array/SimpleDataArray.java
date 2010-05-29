package krati.cds.impl.array;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import krati.cds.Persistable;
import krati.cds.array.DataArray;
import krati.cds.array.LongArray;
import krati.cds.impl.array.SimpleDataArrayCompactor.CompactionUpdateBatch;
import krati.cds.impl.array.entry.Entry;
import krati.cds.impl.array.entry.EntryPersistAdapter;
import krati.cds.impl.array.entry.EntryValue;
import krati.cds.impl.segment.AddressFormat;
import krati.cds.impl.segment.Segment;
import krati.cds.impl.segment.SegmentException;
import krati.cds.impl.segment.SegmentManager;
import krati.cds.impl.segment.SegmentOverflowException;

/**
 * SimpleDataArray: Simple Persistent DataArray.
 * 
 * This class is not thread-safe by design. It is expected that the conditions below hold within one JVM.
 * <pre>
 *    1. There is one and only one instance of SimpleDataArray for a given cacheDirectory.
 *    2. There is one and only one thread is calling setData methods at any given time. 
 * </pre>
 * 
 * It is expected that this class is used in the case of multiple readers and single writer.
 * 
 * @author jwu
 *
 */
public class SimpleDataArray implements DataArray, Persistable
{
    private final static Logger _log = Logger.getLogger(SimpleDataArray.class);
    
    protected Segment _segment;              // current segment to append
    protected boolean _canTriggerCompaction; // current segment can trigger compaction only once
    
    protected volatile AddressArray _addressArray;
    protected volatile SegmentManager _segmentManager;
    protected SimpleDataArrayCompactor _compactor;
    protected ConcurrentLinkedQueue<Segment> _compactedSegmentQueue;
    
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
    public SimpleDataArray(AddressArray addressArray, SegmentManager segmentManager)
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
    public SimpleDataArray(AddressArray addressArray,
                           SegmentManager segmentManager,
                           double segmentCompactTrigger,
                           double segmentCompactFactor)
    {
        this._addressArray = addressArray;
        this._segmentManager = segmentManager;
        this._segmentCompactFactor = segmentCompactFactor;
        this._segmentCompactTrigger = segmentCompactTrigger;
        
        addressArray.setPersistListener(new SegmentPersistListener());
        
        AddressFormat f = new AddressFormat(16);
        this._segmentShift = f.getSegmentShift();
        this._segmentMask = f.getSegmentMask();
        this._offsetMask = f.getOffsetMask();
        
        this.init();

        this._compactor = new SimpleDataArrayCompactor(this, getSegmentCompactFactor());
        this._compactedSegmentQueue = new ConcurrentLinkedQueue<Segment>();
        this._canTriggerCompaction = true;
        this.updateCompactTriggerBounds();
        
        this._metaUpdateOnAppendPosition = Segment.dataStartPosition;
    }
    
    private void consumeCompaction(CompactionUpdateBatch updateBatch) throws Exception 
    {
        int ignoreCount = 0;
        int updateCount = updateBatch.size();
        int totalIgnoreBytes = 0;
        int totalUpdateBytes = updateBatch.getDataSizeTotal();
        int liveSegInd = _segment.getSegmentId();
        
        Segment segTarget = updateBatch.getTargetSegment();
        
        // Update segment append position
        segTarget.setAppendPosition(segTarget.getAppendPosition() + totalUpdateBytes);
        
        // Partial-load data between channelPosition and appendPosition
        segTarget.load();
        
        for(int i = 0; i < updateCount; i++)
        {
            int index = updateBatch.getUpdateIndex(i);
            long address = getAddress(index);
            int segInd = (int)((address >> _segmentShift) & _segmentMask);
            
            if(address == 0 ||      /* data at the given index is deleted by writer */ 
               segInd == liveSegInd /* data at the given index is updated by writer */)
            {
                /*
                 * The address generated by the compactor is obsolete.
                 */
                int updateBytes = updateBatch.getUpdateDataSize(i);
                totalIgnoreBytes += updateBytes;
                ignoreCount++;
            }
            else
            {
                /*
                 * The address generated by the compactor is not updated by the writer.
                 * Update the address array directly.
                 */ 
                setAddress(index, updateBatch.getUpdateAddress(i), updateBatch.getLWMark());
            }
        }
        
        int consumeCount = updateCount - ignoreCount;
        int totalConsumeBytes = totalUpdateBytes - totalIgnoreBytes;
        
        _log.info("consumed compaction batch " + updateBatch.getDescriptiveId() +
                  " updates " + consumeCount + "/" + updateCount +
                  " bytes " + totalConsumeBytes + "/" + totalUpdateBytes);
        
        // Update segment load size
        segTarget.incrLoadSize(totalConsumeBytes);
        _log.info("Segment " + segTarget.getSegmentId() + " catchup " + segTarget.getStatus());
    }
    
    protected void consumeCompaction()
    {
        while(true)
        {
            CompactionUpdateBatch updateBatch = _compactor.pollCompactionBatch();
            if(updateBatch == null) break;
            
            try
            {
                consumeCompaction(updateBatch);
            }
            catch (Exception e)
            {
                _log.error("failed to consume compaction batch " + updateBatch.getDescriptiveId(), e);
            }
            finally
            {
                _compactor.recycleCompactionBatch(updateBatch);
            }
        }
        
        while(!_compactedSegmentQueue.isEmpty())
        {
            Segment seg = _compactedSegmentQueue.remove();
            try
            {
                _segmentManager.freeSegment(seg);
            }
            catch(IOException e)
            {
                _log.error("failed to recycle Segment " + seg.getSegmentId() + ": " + seg.getStatus(), e);
            }
        }
    }
    
    protected void init()
    {
        try
        {
            _segment = _segmentManager.nextSegment();
            _log.info("Segment " + _segment.getSegmentId() + " online: " + _segment.getStatus());
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
        return _addressArray.get(index);
    }
    
    protected void setAddress(int index, long value, long scn) throws Exception
    {
        _addressArray.set(index, value, scn);
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
    
    private void flowControl()
    {
        Segment liveSegment = _segment;
        if(liveSegment == null)
        {
            return;
        }
        
        Segment compactSegment = getSegmentManager().getCurrentSegment();
        if(compactSegment == null || compactSegment == liveSegment)
        {
            return;
        }
        
        /*
         * Slow down the writer for 0.5 milliseconds so that the compactor has a chance to catch up.
         */
        if(compactSegment.getLoadSize() < liveSegment.getLoadSize()) 
        {
            try
            {
                Thread.sleep(0, 500000);
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
            if(seg == null) return -1;
            
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
                
                // append actual size
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
                    // start the compactor
                    if(!_compactor.isStarted())
                    {
                        _log.info("Segment " + _segment.getSegmentId() + " triggered compaction");
                        _canTriggerCompaction = false;
                        _compactor.start();
                    }
                }
                
                if(_compactor.isStarted())
                {
                    /*
                     * Consume one compaction update batch generated by the compactor.
                     */
                    CompactionUpdateBatch updateBatch = _compactor.pollCompactionBatch();
                    if(updateBatch != null)
                    {
                        try
                        {
                            consumeCompaction(updateBatch);
                        }
                        catch (Exception e)
                        {
                            _log.error("failed to consume compaction batch " + updateBatch.getDescriptiveId(), e);
                        }
                        finally
                        {
                            _compactor.recycleCompactionBatch(updateBatch);
                        }
                    }
                    
                    /*
                     * Flow-control the writer and average-out write latency.
                     * Give the compactor a chance to catch up with the writer.
                     */
                    flowControl();
                }
                
                /*
                 * Trigger segment compaction (END)
                 * ******************************************************************************/
                
                return;
            }
            catch(SegmentOverflowException soe)
            {
                _log.info("Segment " + _segment.getSegmentId() + " filled: " + _segment.getStatus());
                
                // wait until compactor is done
                while(_compactor.isStarted())
                {
                    /*
                     * Consume compaction update batches generated by the compactor.
                     */
                    CompactionUpdateBatch updateBatch = _compactor.pollCompactionBatch();
                    if(updateBatch != null)
                    {
                        try
                        {
                            consumeCompaction(updateBatch);
                        }
                        catch (Exception e)
                        {
                            _log.error("failed to consume compaction batch " + updateBatch.getDescriptiveId(), e);
                        }
                        finally
                        {
                            _compactor.recycleCompactionBatch(updateBatch);
                        }
                    }
                    
                    _log.info("wait for compactor");
                    Thread.sleep(10);
                }
                
                // synchronize with the compactor
                _compactor.lock();
                try
                {
                    persist();
                    
                    // get the next segment available for appending
                    _metaUpdateOnAppendPosition = Segment.dataStartPosition;
                    _segment = _segmentManager.nextSegment(_segment);
                    _canTriggerCompaction = true;
                    
                    _log.info("Segment " + _segment.getSegmentId() + " online: " + _segment.getStatus());
                }
                finally
                {
                    _compactor.unlock(); 
                }
            }
            catch(Exception e)
            {
                // restore append position 
                _segment.setAppendPosition(pos);
                _segment.force();
                
                throw e;
            }
        }
    }
    
    @Override
    public boolean hasIndex(int index)
    {
        return _addressArray.hasIndex(index);
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
        consumeCompaction();
        
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
        consumeCompaction();
        
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
        _compactor.reset();
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
