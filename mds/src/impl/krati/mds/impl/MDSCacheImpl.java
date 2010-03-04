package krati.mds.impl;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import krati.mds.MDSCache;
import krati.mds.array.DataArray;
import krati.mds.impl.array.DataArrayImpl;
import krati.mds.impl.array.fixed.LongArrayRecoverableImpl;
import krati.mds.impl.segment.SegmentFactory;
import krati.mds.impl.segment.SegmentManager;

/**
 * MDS (Member Data Service) Cache: Simple Persistent Content Data Service.
 * 
 * @author jwu
 *
 */
public class MDSCacheImpl implements MDSCache
{
    private final static Logger _log = Logger.getLogger(MDSCacheImpl.class);
    private DataArray _dataArray;
    
    /**
     * Constructs a MDS cache with default values below.
     * <pre>
     *    Segment File Size      : 512MB
     *    Segment Compact Trigger: 0.2
     *    Segment Compact Factor : 0.5
     *    Redo entry size        : 10000
     *    Number of Redo Entries : 5
     * </pre>
     * 
     * @param memberIdStart          Start of memberId
     * @param memberIdCount          Total of memberId(s)
     * @param cacheDirectory         Cache directory where persistent data will be stored
     * @param segmentFactory         Factory for creating Segment(s)
     * @throws Exception
     */
    public MDSCacheImpl(int memberIdStart,
                        int memberIdCount,
                        File cacheDirectory,
                        SegmentFactory segmentFactory) throws Exception
    {
        this(memberIdStart, memberIdCount, cacheDirectory, segmentFactory, 512);
    }
    
    /**
     * Constructs a MDS cache with default values below.
     * <pre>
     *    Segment Compact Trigger: 0.2
     *    Segment Compact Factor : 0.5
     *    Redo Entry Size        : 10000
     *    Number of redo entries : 5 
     * </pre>
     * 
     * @param memberIdStart          Start of memberId
     * @param memberIdCount          Total of memberId(s)
     * @param cacheDirectory         Cache directory where persistent data will be stored
     * @param segmentFactory         Factory for creating Segment(s)
     * @param segmentFileSizeMB      Segment size in MB
     * @throws Exception
     */
    public MDSCacheImpl(int memberIdStart,
                        int memberIdCount,
                        File cacheDirectory,
                        SegmentFactory segmentFactory,
                        int segmentFileSizeMB) throws Exception
    {
        this(memberIdStart, memberIdCount, 10000, 5, cacheDirectory, segmentFactory, segmentFileSizeMB);
    }
    
    /**
     * Constructs a MDS cache with Segment Compact Trigger default to 0.2 and Segment Compact Factor default to 0.5.
     * 
     * @param memberIdStart          Start of memberId
     * @param memberIdCount          Total of memberId(s)
     * @param maxEntrySize           Redo entry size (i.e., batch up date)
     * @param maxEntries             Number of redo entries required for updating the underlying address array
     * @param cacheDirectory         Cache directory where persistent data will be stored
     * @param segmentFactory         Factory for creating Segment(s)
     * @param segmentFileSizeMB      Segment size in MB
     * @throws Exception
     */
    public MDSCacheImpl(int memberIdStart,
                        int memberIdCount,
                        int maxEntrySize,
                        int maxEntries,
                        File cacheDirectory,
                        SegmentFactory segmentFactory,
                        int segmentFileSizeMB) throws Exception
    {
        LongArrayRecoverableImpl addressArray =
            new LongArrayRecoverableImpl(memberIdStart,
                                         memberIdCount,
                                         maxEntrySize,
                                         maxEntries,
                                         cacheDirectory);
        
        String segmentHomePath = cacheDirectory.getCanonicalPath() + File.separator + "segs";
        SegmentManager segManager = SegmentManager.getInstance(segmentFactory,
                                                               segmentHomePath,
                                                               segmentFileSizeMB);
        
        _dataArray = new DataArrayImpl(addressArray, segManager);
        
        _log.info("MDSCache initiated: " + getStatus());
    }
    
    /**
     * Constructs a MDS cache.
     * 
     * @param memberIdStart          Start of memberId
     * @param memberIdCount          Total of memberId(s)
     * @param maxEntrySize           Redo entry size (i.e., batch up date)
     * @param maxEntries             Number of redo entries required for updating the underlying address array
     * @param cacheDirectory         Cache directory where persistent data will be stored
     * @param segmentFactory         Factory for creating Segment(s)
     * @param segmentFileSizeMB      Segment size in MB
     * @param segmentCompactTrigger  Percentage of segment capacity, which triggers compaction once per segment
     * @param segmentCompactFactor   Load factor of segment, below which a segment is eligible for compaction
     * @throws Exception
     */
    public MDSCacheImpl(int memberIdStart,
                        int memberIdCount,
                        int maxEntrySize,
                        int maxEntries,
                        File cacheDirectory,
                        SegmentFactory segmentFactory,
                        int segmentFileSizeMB,
                        double segmentCompactTrigger,
                        double segmentCompactFactor) throws Exception
    {
        LongArrayRecoverableImpl addressArray =
            new LongArrayRecoverableImpl(memberIdStart,
                                         memberIdCount,
                                         maxEntrySize,
                                         maxEntries,
                                         cacheDirectory);
        
        String segmentHomePath = cacheDirectory.getCanonicalPath() + File.separator + "segs";
        SegmentManager segManager = SegmentManager.getInstance(segmentFactory,
                                                               segmentHomePath,
                                                               segmentFileSizeMB);
        
        _dataArray = new DataArrayImpl(addressArray, segManager, segmentCompactTrigger, segmentCompactFactor);
        
        _log.info("MDSCache initiated: " + getStatus());
    }
    
    protected String getStatus()
    {
        StringBuffer buffer = new StringBuffer();
        
        buffer.append("idStart");
        buffer.append("=");
        buffer.append(getIdStart());
        buffer.append(" ");
        
        buffer.append("idCount");
        buffer.append("=");
        buffer.append(getIdCount());
        buffer.append(" ");
        
        buffer.append("lwMark");
        buffer.append("=");
        buffer.append(getLWMark());
        buffer.append(" ");

        buffer.append("hwMark");
        buffer.append("=");
        buffer.append(getHWMark());
        
        return buffer.toString();
    }
    
    @Override
    public int getIdCount()
    {
        return _dataArray.length();
    }
    
    @Override
    public int getIdStart()
    {
        return _dataArray.getIndexStart();
    }
    
    @Override
    public byte[] getData(int memberId)
    {
        return _dataArray.getData(memberId);
    }
    
    @Override
    public int getData(int memberId, byte[] dst)
    {
        return _dataArray.getData(memberId, dst);
    }
    
    @Override
    public int getData(int memberId, byte[] dst, int offset)
    {
        return _dataArray.getData(memberId, dst, offset);
    }
    
    @Override
    public void setData(int memberId, byte[] data, long scn) throws Exception
    {
        _dataArray.setData(memberId, data, scn);
    }
    
    @Override
    public void setData(int memberId, byte[] data, int offset, int length, long scn) throws Exception
    {
        _dataArray.setData(memberId, data, offset, length, scn);
    }
    
    @Override
    public void deleteData(int memberId, long scn) throws Exception
    {
        _dataArray.setData(memberId, null, scn);
    }
        
    @Override
    public void persist() throws IOException
    {
        _dataArray.persist();
        
        _log.info("MDSCache persisted: " + getStatus());
    }

    @Override
    public long getHWMark()
    {
        return _dataArray.getHWMark();
    }

    @Override
    public long getLWMark()
    {
        return _dataArray.getLWMark();
    }
    
    @Override
    public void saveHWMark(long endOfPeriod) throws Exception
    {
        _dataArray.saveHWMark(endOfPeriod);
    }
}
