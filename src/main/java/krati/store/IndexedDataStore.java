package krati.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import krati.core.StoreConfig;
import krati.core.segment.MemorySegmentFactory;
import krati.core.segment.SegmentFactory;
import krati.store.DataStore;
import krati.store.index.HashIndex;
import krati.store.index.Index;
import krati.util.IndexedIterator;

/**
 * IndexedDataStore.
 * 
 * @author jwu
 * 
 * <p>
 * 06/04, 2011 - Added support for Closeable <br/>
 * 06/04, 2011 - Added method getHomeDir() <br/>
 * 08/21, 2011 - Added constructors using initialCapacity <br/>
 * 12/05, 2011 - Constructor API cleanup <br/>
 */
public class IndexedDataStore implements DataStore<byte[], byte[]> {
    private final static Logger _logger = Logger.getLogger(IndexedDataStore.class);
    private final BytesDB _bytesDB;
    private final Index _index;
    private final File _homeDir;
    private final File _indexHome;
    private final File _storeHome;
    private final int _batchSize;
    
    private volatile int _updateCnt;
    
    /**
     * Creates a new IndexedDataStore instance.
     * The created store has the following parameters:
     * 
     * <pre>
     *   Index cached :             : true
     *   Index segmentFileSizeMB    : 8
     *   Index segmentFactory       : krati.segment.MemorySegmentFactory
     * </pre>
     * 
     * @param config - the store configuration
     * @throws Exception if the store cannot be created.
     */
    public IndexedDataStore(StoreConfig config) throws Exception {
        this._homeDir = config.getHomeDir();
        this._batchSize = config.getBatchSize();
        
        // Create bytesDB
        _storeHome = new File(_homeDir, "store");
        
        int storeInitialCapacity = config.getInitialCapacity();
        StoreConfig storeConfig = new StoreConfig(_storeHome, storeInitialCapacity);
        storeConfig.setIndexesCached(config.getIndexesCached());
        storeConfig.setBatchSize(config.getBatchSize());
        storeConfig.setNumSyncBatches(config.getNumSyncBatches());
        storeConfig.setSegmentFileSizeMB(config.getSegmentFileSizeMB());
        storeConfig.setSegmentFactory(config.getSegmentFactory());
        storeConfig.setSegmentCompactFactor(config.getSegmentCompactFactor());
        _bytesDB = new BytesDB(storeConfig);
        
        // Create hash index
        _indexHome = new File(_homeDir, "index");
        
        int indexInitialCapacity = config.getInitialCapacity();
        StoreConfig indexConfig = new StoreConfig(_indexHome, indexInitialCapacity);
        indexConfig.setIndexesCached(true);                         // indexes.dat is cached
        indexConfig.setBatchSize(config.getBatchSize());
        indexConfig.setNumSyncBatches(config.getNumSyncBatches());
        indexConfig.setSegmentCompactFactor(config.getSegmentCompactFactor());
        indexConfig.setSegmentFileSizeMB(8);                        // index segment size is 8 MB
        indexConfig.setSegmentFactory(new MemorySegmentFactory());  // index segment is MemorySegment
        indexConfig.setHashLoadFactor(config.getHashLoadFactor());
        indexConfig.setHashFunction(config.getHashFunction());
        indexConfig.setDataHandler(config.getDataHandler());
        _index = new HashIndex(indexConfig);
        
        _logger.info("opened indexHome=" + _indexHome.getAbsolutePath() + " storeHome=" + _storeHome.getAbsolutePath());
    }
    
    /**
     * Creates a new IndexedDataStore instance.
     * The created store has the following parameters:
     * 
     * <pre>
     *   Index segmentFileSizeMB    : 8
     *   Index segmentCompactFactor : 0.5
     *   Index hashLoadFactor       : 0.75
     *   Index hashFunction         : krati.util.FnvHashFunction
     *   BytesDB segmentFileSizeMB  : 256
     * </pre>
     * 
     * @param homeDir                - the home directory of IndexedDataStore
     * @param initialCapacity        - the initial store capacity, which should not be changed after the store is created
     * @param batchSize              - the number of updates per update batch
     * @param numSyncBatches         - the number of update batches required for updating <code>indexes.dat</code>
     * @param indexSegmentFactory    - the segment factory for HashIndex
     * @param storeSegmentFactory    - the segment factory for BytesDB
     * 
     * @throws Exception if the store cannot be created.
     */
    public IndexedDataStore(File homeDir,
                            int initialCapacity,
                            int batchSize, int numSyncBatches,
                            SegmentFactory indexSegmentFactory,
                            SegmentFactory storeSegmentFactory) throws Exception {
        this(homeDir,
             initialCapacity,
             batchSize,
             numSyncBatches,
             8,   // indexSegmentFileSizeMB
             indexSegmentFactory,
             256, // storeSegmentFileSizeMB
             storeSegmentFactory);
    }
    
    /**
     * Creates a new IndexedDataStore instance.
     * The created store has the following parameters:
     * 
     * <pre>
     *   Index segmentCompactFactor : 0.5
     *   Index hashLoadFactor       : 0.75
     *   Index hashFunction         : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir                - the home directory of IndexedDataStore
     * @param initialCapacity        - the initial store capacity, which should not be changed after the store is created
     * @param batchSize              - the number of updates per update batch
     * @param numSyncBatches         - the number of update batches required for updating <code>indexes.dat</code>
     * @param indexSegmentFileSizeMB - the size of index segment in MB 
     * @param indexSegmentFactory    - the segment factory for HashIndex
     * @param storeSegmentFileSizeMB - the size of store segment in MB
     * @param storeSegmentFactory    - the segment factory for BytesDB
     * 
     * @throws Exception if the store cannot be created.
     */
    public IndexedDataStore(File homeDir,
                            int initialCapacity,
                            int batchSize, int numSyncBatches,
                            int indexSegmentFileSizeMB, SegmentFactory indexSegmentFactory,
                            int storeSegmentFileSizeMB, SegmentFactory storeSegmentFactory) throws Exception {
        this._homeDir = homeDir;
        this._batchSize = batchSize;
        
        // Create bytesDB
        _storeHome = new File(homeDir, "store");
        
        int storeInitialCapacity = initialCapacity;
        StoreConfig storeConfig = new StoreConfig(_storeHome, storeInitialCapacity);
        storeConfig.setBatchSize(batchSize);
        storeConfig.setNumSyncBatches(numSyncBatches);
        storeConfig.setSegmentFileSizeMB(storeSegmentFileSizeMB);
        storeConfig.setSegmentFactory(storeSegmentFactory);
        _bytesDB = new BytesDB(storeConfig);
        
        // Create index
        _indexHome = new File(homeDir, "index");
        
        int indexInitialCapacity = initialCapacity;
        StoreConfig indexConfig = new StoreConfig(_indexHome, indexInitialCapacity);
        indexConfig.setBatchSize(batchSize);
        indexConfig.setNumSyncBatches(numSyncBatches);
        indexConfig.setSegmentFileSizeMB(indexSegmentFileSizeMB);
        indexConfig.setSegmentFactory(indexSegmentFactory);
        _index = new HashIndex(indexConfig);
        
        _logger.info("opened indexHome=" + _indexHome.getAbsolutePath() + " storeHome=" + _storeHome.getAbsolutePath());
    }
    
    /**
     * @return the home directory of this data store.
     */
    public final File getHomeDir() {
        return _homeDir;
    }
    
    /**
     * @return the index home directory of this data store.
     */
    public final File getIndexHome() {
        return _indexHome;
    }
    
    /**
     * @return the store home directory of this data store.
     */
    public final File getStoreHome() {
        return _storeHome;
    }
    
    @Override
    public final int capacity() {
        return _index.capacity();
    }
    
    @Override
    public byte[] get(byte[] key) {
        if(key == null) return null;
        
        byte[] metaBytes = _index.lookup(key);
        if(metaBytes == null) return null;
        
        IndexMeta meta = IndexMeta.parse(metaBytes);
        if(meta == null) return null;
        
        return _bytesDB.get(meta.getDataAddr());
    }
    
    @Override
    public synchronized boolean put(byte[] key, byte[] value) throws Exception {
        if(value == null) return delete(key);
        if(key == null) return false;
        
        // Lookup index meta
        IndexMeta meta = null;
        byte[] metaBytes = _index.lookup(key);
        if(metaBytes != null) {
            meta = IndexMeta.parse(metaBytes);
        }
        
        // Update index if needed
        if(meta == null) {
            // Add to bytes DB
            int index = _bytesDB.add(value, System.currentTimeMillis());
            metaBytes = IndexMeta.build(index);
            
            // Update hashIndex
            _index.update(key, metaBytes);
        } else {
            // Update bytes DB
            int index = meta.getDataAddr();
            _bytesDB.set(index, value, System.currentTimeMillis());
            
            // No need to update hashIndex
        }
        
        _updateCnt++;
        if(_updateCnt >= _batchSize) { 
            _updateCnt = 0;
            persist();
        }
        
        return true;
    }
    
    @Override
    public synchronized boolean delete(byte[] key) throws Exception {
        if(key == null) return false;
        
        // Lookup index meta
        byte[] metaBytes = _index.lookup(key);
        if(metaBytes == null) return false;
        
        IndexMeta meta = IndexMeta.parse(metaBytes);
        if(meta == null) return false;
        
        // Delete from bytes DB
        _bytesDB.set(meta.getDataAddr(), null, System.currentTimeMillis());
        
        // Update index 
        _index.update(key, null);
        
        _updateCnt++;
        if(_updateCnt >= _batchSize) { 
            _updateCnt = 0;
            persist();
        }
        
        return true;
    }
    
    @Override
    public synchronized void clear() throws IOException {
        _bytesDB.clear();
        _index.clear();
    }
    
    @Override
    public synchronized void persist() throws IOException {
        _bytesDB.persist();
        _index.persist();
    }
    
    @Override
    public synchronized void sync() throws IOException {
        _bytesDB.sync();
        _index.sync();
    }
    
    static class IndexMeta {
        final int _dataAddr;
        final static int META_SIZE = 4;
        
        IndexMeta(int dataAddr) {
            this._dataAddr = dataAddr;
        }
        
        static byte[] build(int dataAddr) {
            ByteBuffer bb = ByteBuffer.allocate(META_SIZE);
            bb.putInt(dataAddr);
            return bb.array();
        }
        
        static IndexMeta parse(byte[] metaBytes) {
            if(metaBytes.length != META_SIZE) return null;
            ByteBuffer bb = ByteBuffer.wrap(metaBytes);
            int dataAddr = bb.getInt();
            return new IndexMeta(dataAddr);
        }
        
        int getDataAddr() {
            return _dataAddr;
        }
    }

    @Override
    public IndexedIterator<byte[]> keyIterator() {
        if(isOpen()) {
            return _index.keyIterator();
        }
        
        throw new StoreClosedException();
    }

    @Override
    public IndexedIterator<Entry<byte[], byte[]>> iterator() {
        if(isOpen()) {
            return new IndexedDataStoreIterator(_index.iterator());
        }
        
        throw new StoreClosedException();
    }
    
    private class IndexedDataStoreIterator implements IndexedIterator<Entry<byte[], byte[]>> {
        final IndexedIterator<Entry<byte[], byte[]>> _indexIter;
        
        IndexedDataStoreIterator(IndexedIterator<Entry<byte[], byte[]>> indexIter) {
            this._indexIter = indexIter;
        }

        @Override
        public boolean hasNext() {
            return _indexIter.hasNext();
        }

        @Override
        public Entry<byte[], byte[]> next() {
            Entry<byte[], byte[]> keyMeta = _indexIter.next();
            
            if(keyMeta != null) {
                IndexMeta meta = IndexMeta.parse(keyMeta.getValue());
                if(meta == null) return null;
                
                byte[] value = _bytesDB.get(meta.getDataAddr());
                return new AbstractMap.SimpleEntry<byte[], byte[]>(keyMeta.getKey(), value);
            }
            
            return null;
        }
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public int index() {
            return _indexIter.index();
        }
        
        @Override
        public void reset(int indexStart) {
            _indexIter.reset(indexStart);
        }
    }
    
    @Override
    public boolean isOpen() {
        return _index.isOpen();
    }
    
    @Override
    public synchronized void open() throws IOException {
        try {
            _bytesDB.open();
            _index.open();
        } catch(IOException ioe) {
            _index.close();
            _bytesDB.close();
            throw ioe;
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        try {
            _bytesDB.close();
            _index.close();
        } catch(IOException ioe) {
            _index.close();
            throw ioe;
        }
    }
}
