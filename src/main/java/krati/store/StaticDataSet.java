package krati.store;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import krati.array.DataArray;
import krati.core.StoreConfig;
import krati.core.array.AddressArray;
import krati.core.array.AddressArrayFactory;
import krati.core.array.SimpleDataArray;
import krati.core.segment.SegmentFactory;
import krati.core.segment.SegmentManager;
import krati.store.DataSet;
import krati.store.DataSetHandler;
import krati.util.FnvHashFunction;
import krati.util.HashFunction;

/**
 * A simple implementation of data set with a fixed capacity.
 * 
 * The values are stored in the underlying DataArray using the following format:
 * <pre>
 * [count:int][value-length:int][value:bytes][value-length:int][value:bytes]...
 *            +-----------value 1------------+-----------value 2-----------+
 * </pre>
 * 
 * @author jwu
 * 
 * <p>
 * 06/06, 2011 - Added support for Closeable
 * 06/25, 2011 - Added constructor using StoreConfig
 */
public class StaticDataSet implements DataSet<byte[]> {
    private final static Logger _log = Logger.getLogger(StaticDataSet.class);
    
    private final File _homeDir;
    private final StoreConfig _config;
    private final SimpleDataArray _dataArray;
    private final DataSetHandler _dataHandler;
    private final HashFunction<byte[]> _hashFunction;
    
    /**
     * Constructs a static DataStore instance. 
     * 
     * @param config - DataSet configuration
     * @throws Exception if the set can not be created.
     */
    public StaticDataSet(StoreConfig config) throws Exception {
        config.validate();
        config.save();
        
        this._config = config;
        this._homeDir = _config.getHomeDir();
        
        // Create data set handler
        _dataHandler = new DefaultDataSetHandler();
        
        // Create address array
        AddressArray addressArray = createAddressArray(
                _config.getHomeDir(),
                _config.getInitialCapacity(),
                _config.getBatchSize(),
                _config.getNumSyncBatches(),
                _config.getIndexesCached());
        
        if (addressArray.length() != _config.getInitialCapacity()) {
            addressArray.close();
            throw new IOException("Capacity expected: " + addressArray.length() + " not " + _config.getInitialCapacity());
        }
        
        // Create segment manager
        String segmentHome = _homeDir.getCanonicalPath() + File.separator + "segs";
        SegmentManager segmentManager = SegmentManager.getInstance(
                segmentHome,
                _config.getSegmentFactory(),
                _config.getSegmentFileSizeMB());
        
        this._dataArray = new SimpleDataArray(addressArray, segmentManager, _config.getSegmentCompactFactor());
        this._hashFunction = _config.getHashFunction();
    }
    
    /**
     * Creates a DataSet instance with the settings below:
     * 
     * <pre>
     *    batchSize            : 10000
     *    numSyncBatche        : 5
     *    segmentFileSizeMB    : 256
     *    segmentCompactFactor : 0.5
     *    hashFunction         : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir            the home directory
     * @param capacity           the capacity of data set
     * @param segmentFactory     the segment factory
     * @throws Exception
     */
    public StaticDataSet(File homeDir, int capacity, SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             capacity,
             10000,
             5,
             256,
             segmentFactory,
             0.5, /* segment compact factor  */
             new FnvHashFunction());
    }
    
    /**
     * Creates a DataSet instance with the settings below:
     * 
     * <pre>
     *    batchSize            : 10000
     *    numSyncBatches       : 5
     *    segmentCompactFactor : 0.5
     *    hashFunction         : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir            the home directory
     * @param capacity           the capacity of data set
     * @param segmentFileSizeMB  the size of segment file in MB
     * @param segmentFactory     the segment factory
     * @throws Exception
     */
    public StaticDataSet(File homeDir,
                         int capacity,
                         int segmentFileSizeMB,
                         SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             capacity,
             10000,
             5,
             256,
             segmentFactory,
             0.5, /* segment compact factor  */
             new FnvHashFunction());
    }
    
    /**
     * Creates a DataSet instance with the settings below:
     * 
     * <pre>
     *    segmentCompactFactor : 0.5
     *    hashFunction         : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir            the home directory
     * @param capacity           the capacity of data set
     * @param batchSize          the number of updates per update batch
     * @param numSyncBatches     the number of update batches required for updating <code>indexes.dat</code>
     * @param segmentFileSizeMB  the size of segment file in MB
     * @param segmentFactory     the segment factory
     * @throws Exception
     */
    public StaticDataSet(File homeDir,
                         int capacity,
                         int batchSize,
                         int numSyncBatches,
                         int segmentFileSizeMB,
                         SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             capacity,
             batchSize,
             numSyncBatches,
             segmentFileSizeMB,
             segmentFactory,
             0.5, /* segment compact factor  */
             new FnvHashFunction());
    }
    
    /**
     * Creates a DataSet instance with the settings below:
     * 
     * <pre>
     *    segmentCompactFactor : 0.5
     * </pre>
     * 
     * @param homeDir            the home directory
     * @param capacity           the capacity of data set
     * @param batchSize          the number of updates per update batch
     * @param numSyncBatches     the number of update batches required for updating <code>indexes.dat</code>
     * @param segmentFileSizeMB  the size of segment file in MB
     * @param segmentFactory     the segment factory
     * @param hashFunction       the hash function for mapping values to indexes
     * @throws Exception
     */
    public StaticDataSet(File homeDir,
                         int capacity,
                         int batchSize,
                         int numSyncBatches,
                         int segmentFileSizeMB,
                         SegmentFactory segmentFactory,
                         HashFunction<byte[]> hashFunction) throws Exception {
        this(homeDir,
             capacity,
             batchSize,
             numSyncBatches,
             segmentFileSizeMB,
             segmentFactory,
             0.5, /* segment compact factor  */
             hashFunction);
    }
    
    /**
     * Creates a DataSet instance.
     * 
     * @param homeDir              the home directory
     * @param capacity             the capacity of data set
     * @param batchSize            the number of updates per update batch
     * @param numSyncBatches       the number of update batches required for updating <code>indexes.dat</code>
     * @param segmentFileSizeMB    the size of segment file in MB
     * @param segmentFactory       the segment factory
     * @param segmentCompactFactor the load factor of segment, below which a segment is eligible for compaction
     * @param hashFunction         the hash function for mapping values to indexes
     * @throws Exception
     */
    public StaticDataSet(File homeDir,
                         int capacity,
                         int batchSize,
                         int numSyncBatches,
                         int segmentFileSizeMB,
                         SegmentFactory segmentFactory,
                         double segmentCompactFactor,
                         HashFunction<byte[]> hashFunction) throws Exception {
        this._homeDir = homeDir;
        
        // Create/validate/store config
        _config = new StoreConfig(_homeDir, capacity);
        _config.setBatchSize(batchSize);
        _config.setNumSyncBatches(numSyncBatches);
        _config.setSegmentFactory(segmentFactory);
        _config.setSegmentFileSizeMB(segmentFileSizeMB);
        _config.setSegmentCompactFactor(segmentCompactFactor);
        _config.setHashFunction(hashFunction);
        _config.validate();
        _config.save();
        
        // Create data set handler
        _dataHandler = new DefaultDataSetHandler();
        
        // Create address array
        AddressArray addressArray = createAddressArray(
                _config.getHomeDir(),
                _config.getInitialCapacity(),
                _config.getBatchSize(),
                _config.getNumSyncBatches(),
                _config.getIndexesCached());
        
        if (addressArray.length() != capacity) {
            addressArray.close();
            throw new IOException("Capacity expected: " + addressArray.length() + " not " + capacity);
        }
        
        // Create segment manager
        String segmentHome = _homeDir.getCanonicalPath() + File.separator + "segs";
        SegmentManager segmentManager = SegmentManager.getInstance(
                segmentHome,
                _config.getSegmentFactory(),
                _config.getSegmentFileSizeMB());
        
        this._dataArray = new SimpleDataArray(addressArray, segmentManager, _config.getSegmentCompactFactor());
        this._hashFunction = _config.getHashFunction();
    }
    
    protected AddressArray createAddressArray(File homeDir,
                                              int length,
                                              int batchSize,
                                              int numSyncBatches,
                                              boolean indexesCached) throws Exception {
        AddressArrayFactory factory = new AddressArrayFactory(indexesCached);
        AddressArray addrArray = factory.createStaticAddressArray(homeDir, length, batchSize, numSyncBatches);
        return addrArray;
    }
    
    protected long hash(byte[] value) {
        return _hashFunction.hash(value);
    }
    
    protected long nextScn() {
        return System.currentTimeMillis();
    }
    
    @Override
    public final int capacity() {
        return _dataArray.length();
    }
    
    @Override
    public void sync() throws IOException {
        _dataArray.sync();
    }
    
    @Override
    public void persist() throws IOException {
        _dataArray.persist();
    }
    
    @Override
    public boolean has(byte[] value) {
        if(value == null) return false;
        
        long hashCode = hash(value);
        int index = (int)(hashCode % _dataArray.length());
        if (index < 0) index = -index;
        
        byte[] existingData = _dataArray.get(index);
        return existingData == null ? false : _dataHandler.find(value, existingData);
    }
    
    public final int countCollisions(byte[] value) {
        if(value == null) return 0;
        
        long hashCode = hash(value);
        int index = (int)(hashCode % _dataArray.length());
        if (index < 0) index = -index;
        
        byte[] existingData = _dataArray.get(index);
        return existingData == null ? 0 : _dataHandler.countCollisions(value, existingData);
    }
    
    public final boolean hasWithoutCollisions(byte[] value) {
        return countCollisions(value) == 1;
    }
    
    @Override
    public synchronized boolean add(byte[] value) throws Exception {
        if(value == null) return false;
        
        long hashCode = hash(value);
        int index = (int)(hashCode % _dataArray.length());
        if (index < 0) index = -index;
        
        byte[] existingData = _dataArray.get(index);
        
        try {
            if (existingData == null || existingData.length == 0) {
                _dataArray.set(index, _dataHandler.assemble(value), nextScn());
            } else {
                if (!_dataHandler.find(value, existingData)) {
                    _dataArray.set(index, _dataHandler.assemble(value, existingData), nextScn());
                }
            }
        } catch (Exception e) {
            _log.warn("Value reset at index=" + index + " value=\"" + new String(value) + "\"", e);
            _dataArray.set(index, _dataHandler.assemble(value), nextScn());
        }
        
        return true;
    }
    
    @Override
    public synchronized boolean delete(byte[] value) throws Exception {
        if(value == null) return false;
        
        long hashCode = hash(value);
        int index = (int)(hashCode % _dataArray.length());
        if (index < 0) index = -index;
        
        try {
            byte[] existingData = _dataArray.get(index);
            if (existingData != null) {
                int newLength = _dataHandler.remove(value, existingData);
                if (newLength == 0) {
                    // entire data is removed
                    _dataArray.set(index, null, nextScn());
                    return true;
                } else if (newLength < existingData.length) {
                    // partial data is removed
                    _dataArray.set(index, existingData, 0, newLength, nextScn());
                    return true;
                }
            }
        } catch (Exception e) {
            _log.warn("Failed to delete value=\"" + new String(value) + "\"", e);
            _dataArray.set(index, null, nextScn());
        }
        
        // no data is removed
        return false;
    }
    
    @Override
    public synchronized void clear() throws IOException {
        _dataArray.clear();
    }
    
    /**
     * @return the home directory of this data set.
     */
    public final File getHomeDir() {
        return _homeDir;
    }
    
    /**
     * @return the underlying data array.
     */
    public final DataArray getDataArray() {
        return _dataArray;
    }
    
    @Override
    public boolean isOpen() {
        return _dataArray.isOpen();
    }
    
    @Override
    public synchronized void open() throws IOException {
        if(!_dataArray.isOpen()) {
            _dataArray.open();
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(_dataArray.isOpen()) {
            _dataArray.close();
        }
    }
}
