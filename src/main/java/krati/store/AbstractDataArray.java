package krati.store;

import java.io.File;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import krati.Persistable;
import krati.array.Array;
import krati.array.DataArray;
import krati.core.StoreConfig;
import krati.core.array.AddressArray;
import krati.core.array.SimpleDataArray;
import krati.core.segment.SegmentFactory;
import krati.core.segment.SegmentManager;

/**
 * AbstractDataArray
 * 
 * @author jwu
 * 09/24, 2010
 * 
 * <p>
 * 06/25, 2011 - Added support for StoreConfig
 */
public abstract class AbstractDataArray implements DataArray, Persistable {
    protected final SimpleDataArray _dataArray;
    protected final AddressArray _addrArray;
    protected final StoreConfig _config;
    protected final String _homePath;
    protected final File _homeDir;
    
    protected AbstractDataArray(StoreConfig config) throws Exception {
        config.validate();
        config.save();
        
        this._config = config;
        this._homeDir = _config.getHomeDir();
        this._homePath = _homeDir.getCanonicalPath();
        
        // Create address array 
        _addrArray = createAddressArray(
                _config.getHomeDir(),
                _config.getInitialCapacity(),
                _config.getBatchSize(),
                _config.getNumSyncBatches(),
                _config.isIndexesCached());
        
        // Create segment manager
        String segmentHome = _homePath + File.separator + "segs";
        SegmentManager segmentManager = SegmentManager.getInstance(
                segmentHome,
                _config.getSegmentFactory(),
                _config.getSegmentFileSizeMB());
        
        // Create data array
        _dataArray = new SimpleDataArray(_addrArray, segmentManager, _config.getSegmentCompactFactor());
    }
    
    /**
     * Constructs a data array.
     * 
     * @param length               - the array length
     * @param batchSize            - the number of updates per update batch
     * @param numSyncBatches       - the number of update batches required for updating <code>indexes.dat</code>
     * @param homeDirectory        - the home directory of data array
     * @param segmentFactory       - the segment factory
     * @param segmentFileSizeMB    - the segment size in MB
     * @param segmentCompactFactor - the segment load threshold, below which a segment is eligible for compaction
     * @throws Exception
     */
    protected AbstractDataArray(int length,
                                int batchSize,
                                int numSyncBatches,
                                File homeDirectory,
                                SegmentFactory segmentFactory,
                                int segmentFileSizeMB,
                                double segmentCompactFactor) throws Exception {
        this._homeDir = homeDirectory;
        this._homePath = homeDirectory.getCanonicalPath();
        
        // Create/validate/store config
        _config = new StoreConfig(_homeDir, length);
        _config.setBatchSize(batchSize);
        _config.setNumSyncBatches(numSyncBatches);
        _config.setSegmentFactory(segmentFactory);
        _config.setSegmentFileSizeMB(segmentFileSizeMB);
        _config.setSegmentCompactFactor(segmentCompactFactor);
        _config.validate();
        _config.save();
        
        // Create address array 
        _addrArray = createAddressArray(
                _config.getHomeDir(),
                _config.getInitialCapacity(),
                _config.getBatchSize(),
                _config.getNumSyncBatches(),
                _config.isIndexesCached());
        
        // Create segment manager
        String segmentHome = _homePath + File.separator + "segs";
        SegmentManager segmentManager = SegmentManager.getInstance(
                segmentHome,
                _config.getSegmentFactory(),
                _config.getSegmentFileSizeMB());
        
        // Create data array
        _dataArray = new SimpleDataArray(_addrArray, segmentManager, _config.getSegmentCompactFactor());
    }
    
    protected abstract AddressArray createAddressArray(File homeDir,
                                                       int length,
                                                       int batchSize,
                                                       int numSyncBatches,
                                                       boolean indexesCached) throws Exception;
    
    public File getHomeDir() {
        return _homeDir;
    }
    
    public String getHomePath() {
        return _homePath;
    }
    
    public String getStatus() {
        StringBuilder buffer = new StringBuilder();
        
        buffer.append("path");
        buffer.append("=");
        buffer.append(_homePath);
        buffer.append(" ");
        
        buffer.append("length");
        buffer.append("=");
        buffer.append(length());
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
    public int length() {
        return _dataArray.length();
    }

    @Override
    public boolean hasIndex(int index) {
        return _dataArray.hasIndex(index);
    }
    
    @Override
    public byte[] get(int index) {
        return _dataArray.get(index);
    }
    
    @Override
    public int get(int index, byte[] dst) {
        return _dataArray.get(index, dst);
    }
    
    @Override
    public int get(int index, byte[] dst, int offset) {
        return _dataArray.get(index, dst, offset);
    }
    
    @Override
    public int getLength(int index) {
        return _dataArray.getLength(index);
    }
    
    @Override
    public boolean hasData(int index) {
        return _dataArray.hasData(index);
    }
    
    @Override
    public synchronized void set(int index, byte[] data, long scn) throws Exception {
        _dataArray.set(index, data, scn);
    }
    
    @Override
    public synchronized void set(int index, byte[] data, int offset, int length, long scn) throws Exception {
        _dataArray.set(index, data, offset, length, scn);
    }
    
    @Override
    public int transferTo(int index, WritableByteChannel channel) {
        return _dataArray.transferTo(index, channel);
    }
    
    @Override
    public synchronized void clear() {
        _dataArray.clear();
    }
    
    @Override
    public long getLWMark() {
        return _dataArray.getLWMark();
    }
    
    @Override
    public long getHWMark() {
        return _dataArray.getHWMark();
    }
    
    @Override
    public synchronized void saveHWMark(long endOfPeriod) throws Exception {
        _dataArray.saveHWMark(endOfPeriod);
    }
    
    @Override
    public synchronized void persist() throws IOException {
        _dataArray.persist();
    }
    
    @Override
    public synchronized void sync() throws IOException {
        _dataArray.sync();
    }
    
    @Override
    public final Array.Type getType() {
        return _addrArray.getType();
    }
}
