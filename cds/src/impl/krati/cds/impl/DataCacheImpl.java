package krati.cds.impl;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import krati.cds.DataCache;
import krati.cds.array.DataArray;
import krati.cds.impl.array.CheckedDataArrayImpl;
import krati.cds.impl.array.DataArrayImpl;
import krati.cds.impl.array.fixed.LongArrayRecoverableImpl;
import krati.cds.impl.segment.MemorySegmentFactory;
import krati.cds.impl.segment.SegmentFactory;
import krati.cds.impl.segment.SegmentManager;

/**
 * DataCache: Simple Persistent Content Data Service.
 * 
 * @author jwu
 *
 */
public class DataCacheImpl implements DataCache
{
    private final static Logger _log = Logger.getLogger(DataCacheImpl.class);
    private DataArray _dataArray;
    
    /**
     * Constructs a data cache with default values below.
     * <pre>
     *    Segment File Size      : 512MB
     *    Segment Compact Trigger: 0.2
     *    Segment Compact Factor : 0.5
     *    Redo entry size        : 10000
     *    Number of Redo Entries : 5
     *    Data integrity check   : No
     *    Segment Factory        : MemorySegmentFactory
     * </pre>
     * 
     * @param memberIdStart          Start of memberId
     * @param memberIdCount          Total of memberId(s)
     * @param cacheDirectory         Cache directory where persistent data will be stored
     * @throws Exception
     */
    public DataCacheImpl(int memberIdStart,
                         int memberIdCount,
                         File cacheDirectory) throws Exception
    {
        this(memberIdStart, memberIdCount, cacheDirectory, new MemorySegmentFactory(), 512);
    }
    
    /**
     * Constructs a data cache with default values below.
     * <pre>
     *    Segment Compact Trigger: 0.2
     *    Segment Compact Factor : 0.5
     *    Redo entry size        : 10000
     *    Number of Redo Entries : 5
     *    Data integrity check   : No
     *    Segment Factory        : MemorySegmentFactory
     * </pre>
     * 
     * @param memberIdStart          Start of memberId
     * @param memberIdCount          Total of memberId(s)
     * @param cacheDirectory         Cache directory where persistent data will be stored
     * @param segmentFileSizeMB      Segment size in MB
     * @throws Exception
     */
    public DataCacheImpl(int memberIdStart,
                         int memberIdCount,
                         File cacheDirectory,
                         int segmentFileSizeMB) throws Exception
    {
        this(memberIdStart, memberIdCount, cacheDirectory, new MemorySegmentFactory(), segmentFileSizeMB);
    }
    
    /**
     * Constructs a data cache with default values below.
     * <pre>
     *    Segment File Size      : 512MB
     *    Segment Compact Trigger: 0.2
     *    Segment Compact Factor : 0.5
     *    Redo entry size        : 10000
     *    Number of Redo Entries : 5
     *    Data integrity check   : No
     * </pre>
     * 
     * @param memberIdStart          Start of memberId
     * @param memberIdCount          Total of memberId(s)
     * @param cacheDirectory         Cache directory where persistent data will be stored
     * @param segmentFactory         Factory for creating Segment(s)
     * @throws Exception
     */
    public DataCacheImpl(int memberIdStart,
                         int memberIdCount,
                         File cacheDirectory,
                         SegmentFactory segmentFactory) throws Exception
    {
        this(memberIdStart, memberIdCount, cacheDirectory, segmentFactory, 512);
    }
    
    /**
     * Constructs a data cache with default values below.
     * <pre>
     *    Segment Compact Trigger: 0.2
     *    Segment Compact Factor : 0.5
     *    Redo Entry Size        : 10000
     *    Number of redo entries : 5
     *    Data integrity check   : No
     * </pre>
     * 
     * @param memberIdStart          Start of memberId
     * @param memberIdCount          Total of memberId(s)
     * @param cacheDirectory         Cache directory where persistent data will be stored
     * @param segmentFactory         Factory for creating Segment(s)
     * @param segmentFileSizeMB      Segment size in MB
     * @throws Exception
     */
    public DataCacheImpl(int memberIdStart,
                         int memberIdCount,
                         File cacheDirectory,
                         SegmentFactory segmentFactory,
                         int segmentFileSizeMB) throws Exception
    {
        this(memberIdStart, memberIdCount, 10000, 5, cacheDirectory, segmentFactory, segmentFileSizeMB, false);
    }
    
    /**
     * Constructs a data cache with Segment Compact Trigger default to 0.2 and Segment Compact Factor default to 0.5.
     * 
     * @param memberIdStart          Start of memberId
     * @param memberIdCount          Total of memberId(s)
     * @param maxEntrySize           Redo entry size (i.e., batch up date)
     * @param maxEntries             Number of redo entries required for updating the underlying address array
     * @param cacheDirectory         Cache directory where persistent data will be stored
     * @param segmentFactory         Factory for creating Segment(s)
     * @param segmentFileSizeMB      Segment size in MB
     * @param checked                whether to apply default checksum (Adler32) to ensure data integrity
     * @throws Exception
     */
    public DataCacheImpl(int memberIdStart,
                         int memberIdCount,
                         int maxEntrySize,
                         int maxEntries,
                         File cacheDirectory,
                         SegmentFactory segmentFactory,
                         int segmentFileSizeMB,
                         boolean checked) throws Exception
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
        
        if(checked)
        {
            _dataArray = new CheckedDataArrayImpl(addressArray, segManager);
        }
        else
        {
            _dataArray = new DataArrayImpl(addressArray, segManager);
        }
        
        _log.info("DataCache initiated: " + getStatus());
    }
    
    /**
     * Constructs a data cache.
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
     * @param checked                whether to apply default checksum (Adler32) to ensure data integrity
     * @throws Exception
     */
    public DataCacheImpl(int memberIdStart,
                         int memberIdCount,
                         int maxEntrySize,
                         int maxEntries,
                         File cacheDirectory,
                         SegmentFactory segmentFactory,
                         int segmentFileSizeMB,
                         double segmentCompactTrigger,
                         double segmentCompactFactor,
                         boolean checked) throws Exception
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
        
        if (checked)
        {
            _dataArray = new CheckedDataArrayImpl(addressArray, segManager, segmentCompactTrigger, segmentCompactFactor);
        }
        else
        {
            _dataArray = new DataArrayImpl(addressArray, segManager, segmentCompactTrigger, segmentCompactFactor);
        }
        
        _log.info("DataCache initiated: " + getStatus());
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
        _log.info("DataCache persist: " + getStatus());
        
        _dataArray.persist();
        
        _log.info("DataCache persist ended: " + getStatus());
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

    @Override
    public void clear() throws IOException
    {
        _dataArray.clear();
    }
}
