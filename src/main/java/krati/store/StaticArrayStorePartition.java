package krati.store;

import java.io.File;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import krati.core.StoreParams;
import org.apache.log4j.Logger;

import krati.array.Array;
import krati.core.StorePartitionConfig;
import krati.core.array.AddressArray;
import krati.core.array.AddressArrayFactory;
import krati.core.array.SimpleDataArray;
import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.Segment;
import krati.core.segment.SegmentFactory;
import krati.core.segment.SegmentManager;

/**
 * StaticArrayStorePartition
 * 
 * @author jwu
 * 
 * <p>
 * 05/30, 2011 - Added support for Closeable <br/>
 * 06/03, 2011 - Constructor cleanup <br/>
 * 06/26, 2011 - Added StorePartitionConfig-based constructor <br/>
 */
public class StaticArrayStorePartition implements ArrayStorePartition {
    private final static Logger _log = Logger.getLogger(StaticArrayStorePartition.class);
    private final StorePartitionConfig _config;
    private final SimpleDataArray _dataArray;
    private final int _idCount;
    private final int _idStart;
    private final int _idEnd;
    
    public StaticArrayStorePartition(StorePartitionConfig config) throws Exception {
        config.validate();
        config.save();
        
        this._config = config;
        this._idCount = config.getPartitionCount();
        this._idStart = config.getPartitionStart();
        this._idEnd = config.getPartitionEnd();
        
        AddressArray addressArray = createAddressArray(
                _config.getHomeDir(),
                _idCount,
                _config.getBatchSize(),
                _config.getNumSyncBatches(),
                _config.isIndexesCached());
        
        String segmentHome = _config.getHomeDir().getCanonicalPath() + File.separator + "segs";
        SegmentManager segManager = SegmentManager.getInstance(
                segmentHome,
                _config.getSegmentFactory(),
                _config.getSegmentFileSizeMB());
        
        _dataArray = new SimpleDataArray(addressArray, segManager, _config.getSegmentCompactFactor());
        
        _log.info("init: " + getStatus());
    }
    
    /**
     * Constructs a StaticArrayStorePartition with default values below.
     * <pre>
     *    segmentCompactFactor : 0.5
     *    updateBatchSize      : 10000
     *    numSyncBatches       : 5
     *    checked              : false
     *    segmentFactory       : MappedSegmentFactory
     * </pre>
     * 
     * @param idStart            Start of memberId
     * @param idCount            Total of memberId(s)
     * @param homeDir            Directory where persistent data will be stored
     * @param segmentFileSizeMB  Segment size in MB
     * @throws Exception
     */
    public StaticArrayStorePartition(int idStart, int idCount, File homeDir, int segmentFileSizeMB) throws Exception {
        this(idStart, idCount, homeDir, new MappedSegmentFactory(), segmentFileSizeMB);
    }
    
    /**
     * Constructs a StaticArrayStorePartition with default values below.
     * <pre>
     *    segmentCompactFactor : 0.5
     *    updateBatchSize      : 10000
     *    numSyncBatches       : 5
     *    checked              : false
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
        this(idStart,
             idCount,
             StoreParams.BATCH_SIZE_DEFAULT,
             StoreParams.NUM_SYNC_BATCHES_DEFAULT,
             homeDir,
             segmentFactory,
             segmentFileSizeMB,
             false);
    }
    
    /**
     * Constructs a StaticArrayStorePartition with Segment Compact Factor default to 0.5.
     * 
     * @param idStart              Start of memberId
     * @param idCount              Total of memberId(s)
     * @param batchSize            The number of updates per update batch
     * @param numSyncBatches       The number of update batches required for updating <code>indexes.dat</code>
     * @param homeDir              Directory where persistent data will be stored
     * @param segmentFactory       Factory for creating Segment(s)
     * @param segmentFileSizeMB    Segment size in MB
     * @param checked              Whether to apply default checksum (Adler32) to ensure data integrity
     * @throws Exception
     */
    public StaticArrayStorePartition(int idStart,
                                     int idCount,
                                     int batchSize,
                                     int numSyncBatches,
                                     File homeDir,
                                     SegmentFactory segmentFactory,
                                     int segmentFileSizeMB,
                                     boolean checked) throws Exception {
        this(idStart,
             idCount,
             batchSize,
             numSyncBatches,
             homeDir,
             segmentFactory,
             segmentFileSizeMB,
             Segment.defaultSegmentCompactFactor,
             checked);
    }
    
    /**
     * Constructs a StaticArrayStorePartition.
     * 
     * @param idStart                Start of memberId
     * @param idCount                Total of memberId(s)
     * @param batchSize              The number of updates per update batch
     * @param numSyncBatches         The number of update batches required for updating <code>indexes.dat</code>
     * @param homeDir                Directory where persistent data will be stored
     * @param segmentFactory         Factory for creating Segment(s)
     * @param segmentFileSizeMB      Segment size in MB
     * @param segmentCompactFactor   Load factor of segment, below which a segment is eligible for compaction
     * @param checked                Whether to apply default checksum (Adler32) to ensure data integrity
     * @throws Exception
     */
    public StaticArrayStorePartition(int idStart,
                                     int idCount,
                                     int batchSize,
                                     int numSyncBatches,
                                     File homeDir,
                                     SegmentFactory segmentFactory,
                                     int segmentFileSizeMB,
                                     double segmentCompactFactor,
                                     boolean checked) throws Exception {
        _config = new StorePartitionConfig(homeDir, idStart, idCount);
        _config.setBatchSize(batchSize);
        _config.setNumSyncBatches(numSyncBatches);
        _config.setSegmentFactory(segmentFactory);
        _config.setSegmentFileSizeMB(segmentFileSizeMB);
        _config.setSegmentCompactFactor(segmentCompactFactor);
        _config.validate();
        _config.save();
        
        this._idStart = _config.getPartitionStart();
        this._idCount = _config.getPartitionCount();
        this._idEnd = _config.getPartitionEnd();
        
        AddressArray addressArray = createAddressArray(
                _config.getHomeDir(),
                _idCount,
                _config.getBatchSize(),
                _config.getNumSyncBatches(),
                _config.isIndexesCached());
        
        String segmentHome = _config.getHomeDir().getCanonicalPath() + File.separator + "segs";
        SegmentManager segManager = SegmentManager.getInstance(
                segmentHome,
                _config.getSegmentFactory(),
                _config.getSegmentFileSizeMB());
        
        _dataArray = new SimpleDataArray(addressArray, segManager, _config.getSegmentCompactFactor());
        
        if (checked) {
            // TODO
        }
        
        _log.info("init: " + getStatus());
    }
    
    protected AddressArray createAddressArray(File homeDir,
                                              int length,
                                              int batchSize,
                                              int numSyncBatches,
                                              boolean indexesCached) throws Exception {
        AddressArrayFactory factory = new AddressArrayFactory(indexesCached);
        AddressArray addrArray = factory.createStaticAddressArray(homeDir, length, batchSize, numSyncBatches);
        
        if(addrArray.length() != length) {
            throw new IOException("Capacity expected: " + addrArray.length() + " not " + length);
        }
        
        return addrArray;
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
    
    public final File getHomeDir() {
        return _config.getHomeDir();
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
    public synchronized void set(int index, byte[] data, long scn) throws Exception {
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
        return (_idStart <= index && index < _idEnd);
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
    
    @Override
    public boolean isOpen() {
        return _dataArray.isOpen();
    }
    
    @Override
    public synchronized void open() throws IOException {
        _dataArray.open();
    }
    
    @Override
    public synchronized void close() throws IOException {
        _dataArray.close();
    }
    
    @Override
    public final Array.Type getType() {
        return Array.Type.STATIC;
    }
}
