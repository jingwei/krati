package krati.cds.impl.array;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import krati.cds.impl.array.basic.LongArrayMemoryImpl;
import krati.cds.impl.array.basic.LongArrayRecoverableImpl;
import krati.cds.impl.array.entry.EntryFileWriter;
import krati.cds.impl.segment.Segment;
import krati.cds.impl.segment.SegmentManager;

/**
 * DataArray Compactor
 * 
 * The compaction is a three-stage process: compact, catchup, replace. 
 * 
 * @author jwu
 *
 */
public class DataArrayImplCompactor implements Runnable
{
    private final static Logger _log = Logger.getLogger(DataArrayImplCompactor.class);
    
    private DataArrayImpl _dataArray;
    private DataArrayImpl _dataArrayCopy;
    private final double _compactLoadFactor;
    private volatile State _state = State.DONE;
    
    /**
     * Reclaim segments in _segSourceList and transfer their content to _segTarget.
     */
    private ArrayList<Segment> _segSourceList = null; // List of segments to be compacted
    private ArrayList<Segment> _segTargetList = null; // List of segments filled by the compactor
    
    /**
     * Store updates from _dataArray when compaction is at work.
     * These updates will be consumed by the compactor during the state of catchup.
     */
    private ConcurrentLinkedQueue<AddressUpdate> _updateQueue = null;
    
    private EntryFileWriter _entryWriter = null;
    
    /**
     * Constructs a DataArrayCompactor with the default compact load factor (0.5).
     * 
     * A segment is eligible for compaction only if its load factor is less than
     * the default compact load factor (0.5).
     * 
     * @param dataArray          the data array to compact
     */
    public DataArrayImplCompactor(DataArrayImpl dataArray)
    {
        this(dataArray, 0.5);
    }
    
    /**
     * Constructs a DataArrayCompactor with a specified compact load factor.
     * 
     * A segment is eligible for compaction only if its load factor is less than
     * the user-specified compact load factor.
     * 
     * @param dataArray          the data array to compact
     * @param compactLoadFactor  the load factor below which a segment is eligible for compaction
     */
    public DataArrayImplCompactor(DataArrayImpl dataArray, double compactLoadFactor)
    {
        this._dataArray = dataArray;
        this._compactLoadFactor = compactLoadFactor;
        this._segSourceList = new ArrayList<Segment>();
        this._segTargetList = new ArrayList<Segment>();
        this._updateQueue = new ConcurrentLinkedQueue<AddressUpdate>();
        
        File file = ((LongArrayRecoverableImpl)(dataArray._addressArray)).getEntryManager().getEntryLogFile("compactor");
        this._entryWriter = new EntryFileWriter(file);
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
    
    protected DataArrayImpl getDataArrayCopy()
    {
        return _dataArrayCopy;
    }
    
    private void reset()
    {
        _updateQueue.clear();
        _segSourceList.clear();
        _segTargetList.clear();
        _dataArrayCopy = null;
        
        ((LongArrayRecoverableImpl)_dataArray.getAddressArray()).getEntryManager().setAutoApplyEntries(true);
        
        // Reset _state
        _state = State.DONE;
        _log.info("reset _state=" + _state);
    }
    
    private void compact() throws IOException
    {
        SegmentManager segManager = _dataArray.getSegmentManager();
        Segment segCurrent = _dataArray.getCurrentSegment();
        if(segManager == null) return;
        
        synchronized(segManager)
        {
            _segSourceList.clear();
            _segTargetList.clear();
            
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
            if (recycleList.size() == 0) return;
            
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
            
            for(Segment seg : _segSourceList)
            {
                _log.info("Segment " + seg.getSegmentId() + " load factor=" + ((long)(seg.getLoadFactor() * 10000) / 10000.0));
            }
            
            /*
             * Copy dataArray and its segmentManager.
             */
            LongArrayMemoryImpl addressArrayCopy =
                (LongArrayMemoryImpl)_dataArray.getAddressArray().memoryClone();
            SegmentManager segManagerCopy = (SegmentManager) segManager.clone();
            _dataArrayCopy = new DataArrayImpl(addressArrayCopy, segManagerCopy, _segTargetList);
            
            _log.info("copy created");
        }
        
        // No synchronization on segManager is required as we are dealing with its copy
        
        // Safety check
        if(_dataArrayCopy == null) return;
        
        /*
         * Compact the dataArray copy.
         */
        long hwMark = _dataArray.getHWMark();
        try
        {
            _entryWriter.open(hwMark, hwMark);
            for(Segment seg : _segSourceList)
            {
                long bytesTransferred = _dataArrayCopy.compact(seg, _segTargetList, _entryWriter);
                _log.info("bytes transferred from " + seg.getSegmentId() + ": " + bytesTransferred);
            }
        }
        catch(CompactionAbortedException e)
        {
            _log.warn(e.getMessage());
            return;
        }
        finally
        {
            _entryWriter.close();
        }

        for(Segment seg : _segTargetList)
        {
            seg.load();
            _log.info("bytes transferred to   " + seg.getSegmentId() + ": " + seg.getLoadSize());
        }
        
        _state = State.COMPACT_DONE;
    }
    
    private void catchup() throws Exception
    {
        int cnt = 0;
        
        while(!_updateQueue.isEmpty())
        {
            AddressUpdate update = _updateQueue.remove();
            _dataArrayCopy.catchup(update._index, update._address, update._scn, _segTargetList);
            cnt++;
        }
        
        _log.info("catchup " + cnt + " updates");
    }
    
    private void replace() throws IOException
    {
        _dataArray.wrap(_dataArrayCopy._addressArray, _dataArrayCopy._segmentManager);
        
        // Free source segments only after wrap is done
        for(Segment seg : _segSourceList)
        {
            _dataArrayCopy._segmentManager.freeSegment(seg);
        }
        
        // Delete compactor entry file after a successful replacement is done.
        File file = _entryWriter.getFile();
        if(file != null && file.exists())
        {
            try
            {
                file.delete();
            }
            catch(Exception e)
            {
                _log.warn(e.getMessage());
            }
        }
        
        _state = State.REPLACE_DONE;
    }
    
    @Override
    public void run()
    {
        try
        {
            // One and only one compactor is at work
            synchronized(_dataArray)
            {
                ((LongArrayRecoverableImpl)_dataArray.getAddressArray()).getEntryManager().setAutoApplyEntries(false);
                
                _log.info("compaction started");
                _state = State.INIT;
                
                // Compact the copy
                compact();
                if (_state == State.COMPACT_DONE)
                {
                    _log.info("compact done");
                    
                    // Catch up with new updates (1st round)
                    catchup();
                    _log.info("catchup done (1st round)");
                    
                    synchronized(this) // Synchronize the code below and the method addressUpdated 
                    {
                        /* Need catch up new updates again. This is because new updates might
                         * be queued right after the first round is finished and right before
                         * this compactor is synchronized.
                         * 
                         * In most cases, the second catch up will not do anything.
                         */
                        
                        // Catch up with new updates (2nd round)
                        catchup();
                        _log.info("catchup done (2nd round)");
                        
                        // Replace old data with new copy
                        replace();
                        _log.info("replace done");
                        
                        _state = State.DONE;
                    }
                }
                
                // Notify next compactor (if any) to start
                _dataArray.notify();
                
                _state = State.DONE;
                _log.info("compaction ended");
                
                ((LongArrayRecoverableImpl)_dataArray.getAddressArray()).getEntryManager().setAutoApplyEntries(true);                
            }
        }
        catch(Exception e)
        {
            e.printStackTrace(System.err);
            _log.error("failed to compact: " + e.getMessage());
        }
        finally
        {
            reset();
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
    
    public synchronized void addressUpdated(int index, long address, long scn) throws InterruptedException
    {
        if(_state.ordinal() < State.REPLACE_DONE.ordinal())
        {
            _updateQueue.add(new AddressUpdate(index, address, scn));
        }
    }
    
    static enum State {
        /*
         * Do not change the order below
         */
        INIT,
        COMPACT_DONE,
        REPLACE_DONE,
        DONE;
    }
    
    static class AddressUpdate
    {
        int _index;
        long _address;
        long _scn;
        
        AddressUpdate(int index, long address, long scn)
        {
            this._index = index;
            this._address = address;
            this._scn = scn;
        }
    }
}
