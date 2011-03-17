package krati.store;

import java.io.File;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import org.apache.log4j.Logger;

import krati.core.array.SimpleDataArray;
import krati.core.array.basic.StaticLongArray;
import krati.core.segment.MemorySegmentFactory;
import krati.core.segment.SegmentFactory;
import krati.core.segment.SegmentManager;

/**
 * StaticArrayStorePartition
 * 
 * @author jwu
 * 
 */
public class StaticArrayStorePartition implements ArrayStorePartition {
    private final static Logger _log = Logger.getLogger(StaticArrayStorePartition.class);
    private final SimpleDataArray _dataArray;
    private final int _idCount;
    private final int _idStart;
    private final int _idEnd;
    
    /**
     * Constructs a StaticArrayStorePartition with default values below.
     * <pre>
     *    segmentFileSize      : 256MB
     *    segmentCompactFactor : 0.5
     *    updateBatchSize      : 10000
     *    numSyncBatches       : 5
     *    checked              : No
     *    segmentFactory       : MemorySegmentFactory
     * </pre>
     * 
     * @param idStart            Start of memberId
     * @param idCount            Total of memberId(s)
     * @param homeDir            Directory where persistent data will be stored
     * @throws Exception
     */
    public StaticArrayStorePartition(int idStart, int idCount, File homeDir) throws Exception {
        this(idStart, idCount, homeDir, new MemorySegmentFactory(), 256);
    }
    
    /**
     * Constructs a StaticArrayStorePartition with default values below.
     * <pre>
     *    segmentCompactFactor : 0.5
     *    updateBatchSize      : 10000
     *    numSyncBatches       : 5
     *    checked              : No
     *    segmentFactory       : MemorySegmentFactory
     * </pre>
     * 
     * @param idStart            Start of memberId
     * @param idCount            Total of memberId(s)
     * @param homeDir            Directory where persistent data will be stored
     * @param segmentFileSizeMB  Segment size in MB
     * @throws Exception
     */
    public StaticArrayStorePartition(int idStart, int idCount, File homeDir, int segmentFileSizeMB) throws Exception {
        this(idStart, idCount, homeDir, new MemorySegmentFactory(), segmentFileSizeMB);
    }
    
    /**
     * Constructs a StaticArrayStorePartition with default values below.
     * <pre>
     *    segmentFileSize      : 256MB
     *    segmentCompactFactor : 0.5
     *    updateBatchSize      : 10000
     *    numSyncBatches       : 5
     *    checked              : No
     * </pre>
     * 
     * @param idStart            Start of memberId
     * @param idCount            Total of memberId(s)
     * @param homeDir            Directory where persistent data will be stored
     * @param segmentFactory     Factory for creating Segment(s)
     * @throws Exception
     */
    public StaticArrayStorePartition(int idStart,
                                     int idCount,
                                     File homeDir,
                                     SegmentFactory segmentFactory) throws Exception {
        this(idStart, idCount, homeDir, segmentFactory, 256);
    }
    
    /**
     * Constructs a StaticArrayStorePartition with default values below.
     * <pre>
     *    segmentCompactFactor : 0.5
     *    updateBatchSize      : 10000
     *    numSyncBatches       : 5
     *    checked              : No
     * </pre>
     * 
     * @param idStart            Start of memberId
     * @param idCount            Total of memberId(s)
     * @param homeDir            Directory where persistent data will be stored
     * @param segmentFactory     Factory for creating Segment(s)
     * @param segmentFileSizeMB  Segment size in MB
     * @throws Exception
     */
    public StaticArrayStorePartition(int idStart, int idCount, File homeDir,
                                     SegmentFactory segmentFactory, int segmentFileSizeMB) throws Exception {
        this(idStart, idCount, 10000, 5, homeDir, segmentFactory, segmentFileSizeMB, false);
    }
    
    /**
     * Constructs a StaticArrayStorePartition with Segment Compact Factor default to 0.5.
     * 
     * @param idStart              Start of memberId
     * @param idCount              Total of memberId(s)
     * @param updateBatchSize      Redo entry size (i.e., batch size)
     * @param numSyncBatches       Number of redo entries required for updating the underlying address array
     * @param homeDir              Directory where persistent data will be stored
     * @param segmentFactory       Factory for creating Segment(s)
     * @param segmentFileSizeMB    Segment size in MB
     * @param checked                whether to apply default checksum (Adler32) to ensure data integrity
     * @throws Exception
     */
    public StaticArrayStorePartition(int idStart,
                                     int idCount,
                                     int updateBatchSize,
                                     int numSyncBatches,
                                     File homeDir,
                                     SegmentFactory segmentFactory,
                                     int segmentFileSizeMB,
                                     boolean checked) throws Exception {
        this._idStart = idStart;
        this._idCount = idCount;
        this._idEnd = idStart + idCount;
        
        StaticLongArray addressArray =
            new StaticLongArray(idCount,
                                updateBatchSize,
                                numSyncBatches,
                                homeDir);
        
        if (addressArray.length() != idCount) {
            throw new IOException("Capacity expected: " + addressArray.length() + " not " + idCount);
        }
        
        String segmentHome = homeDir.getCanonicalPath() + File.separator + "segs";
        SegmentManager segManager = SegmentManager.getInstance(segmentHome,
                                                               segmentFactory,
                                                               segmentFileSizeMB);
        _dataArray = new SimpleDataArray(addressArray, segManager);
        
        if (checked) {
            // TODO
        }
        
        _log.info("Partition init: " + getStatus());
    }
    
    /**
     * Constructs a StaticArrayStorePartition.
     * 
     * @param idStart                Start of memberId
     * @param idCount                Total of memberId(s)
     * @param updateBatchSize        Redo entry size (i.e., batch size)
     * @param numSyncBatches         Number of redo entries required for updating the underlying address array
     * @param homeDir                Directory where persistent data will be stored
     * @param segmentFactory         Factory for creating Segment(s)
     * @param segmentFileSizeMB      Segment size in MB
     * @param segmentCompactFactor   Load factor of segment, below which a segment is eligible for compaction
     * @param checked                whether to apply default checksum (Adler32) to ensure data integrity
     * @throws Exception
     */
    public StaticArrayStorePartition(int idStart,
                                     int idCount,
                                     int updateBatchSize,
                                     int numSyncBatches,
                                     File homeDir,
                                     SegmentFactory segmentFactory,
                                     int segmentFileSizeMB,
                                     double segmentCompactFactor,
                                     boolean checked) throws Exception {
        this._idStart = idStart;
        this._idCount = idCount;
        this._idEnd = idStart + idCount;
        
        StaticLongArray addressArray =
            new StaticLongArray(idCount,
                                updateBatchSize,
                                numSyncBatches,
                                homeDir);
        
        if(addressArray.length() != idCount) {
            throw new IOException("Capacity expected: " + addressArray.length() + " not " + idCount);
        }
        
        String segmentHome = homeDir.getCanonicalPath() + File.separator + "segs";
        SegmentManager segManager = SegmentManager.getInstance(segmentHome,
                                                               segmentFactory,
                                                               segmentFileSizeMB);
        
        _dataArray = new SimpleDataArray(addressArray, segManager, segmentCompactFactor);
        
        if (checked) {
            // TODO
        }
        
        _log.info("Partition init: " + getStatus());
    }
    
    protected String getStatus() {
        StringBuilder buffer = new StringBuilder();
        
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
    
    private void rangeCheck(int memberId) {
        if(memberId < _idStart || _idEnd <= memberId)
            throw new ArrayIndexOutOfBoundsException(memberId);
    }
    
    @Override
    public int getIndexStart() {
        return _idStart;
    }
    
    @Override
    public int capacity() {
        return _idCount;
    }
    
    @Override
    public int getIdCount() {
        return _idCount;
    }
    
    @Override
    public int getIdStart() {
        return _idStart;
    }
    
    @Override
    public byte[] get(int index) {
        rangeCheck(index);
        return _dataArray.get(index - _idStart);
    }
    
    @Override
    public int get(int index, byte[] dst) {
        rangeCheck(index);
        return _dataArray.get(index - _idStart, dst);
    }
    
    @Override
    public int get(int index, byte[] dst, int offset) {
        rangeCheck(index);
        return _dataArray.get(index - _idStart, dst, offset);
    }
    
    @Override
    public void set(int index, byte[] data, long scn) throws Exception {
        rangeCheck(index);
        _dataArray.set(index - _idStart, data, scn);
    }
    
    @Override
    public synchronized void set(int index, byte[] data, int offset, int length, long scn) throws Exception {
        rangeCheck(index);
        _dataArray.set(index - _idStart, data, offset, length, scn);
    }
    
    @Override
    public synchronized void delete(int index, long scn) throws Exception {
        rangeCheck(index);
        _dataArray.set(index - _idStart, null, scn);
    }
    
    @Override
    public synchronized void sync() throws IOException {
        _dataArray.sync();
        _log.info("sync: " + getStatus());
    }
    
    @Override
    public synchronized void persist() throws IOException {
        _dataArray.persist();
    }
    
    @Override
    public synchronized void clear() {
        _dataArray.clear();
    }
    
    @Override
    public synchronized void saveHWMark(long endOfPeriod) throws Exception {
        _dataArray.saveHWMark(endOfPeriod);
    }
    
    @Override
    public long getHWMark() {
        return _dataArray.getHWMark();
    }
    
    @Override
    public long getLWMark() {
        return _dataArray.getLWMark();
    }
    
    @Override
    public boolean hasIndex(int index) {
        return (_idStart <= index  && index < _idEnd) ? true : false;
    }
    
    @Override
    public int length() {
        return _dataArray.length();
    }
    
    @Override
    public int getLength(int index) {
        rangeCheck(index);
        return _dataArray.getLength(index - _idStart);
    }
    
    @Override
    public boolean hasData(int index) {
        rangeCheck(index);
        return _dataArray.hasData(index - _idStart);
    }
    
    @Override
    public int transferTo(int index, WritableByteChannel channel) {
        rangeCheck(index);
        return _dataArray.transferTo(index - _idStart, channel);
    }
}
