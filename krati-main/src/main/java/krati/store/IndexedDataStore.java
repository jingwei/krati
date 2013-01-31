/*
 * Copyright (c) 2010-2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package krati.store;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import krati.PersistableListener;
import krati.core.StoreConfig;
import krati.core.StoreParams;
import krati.core.segment.MemorySegmentFactory;
import krati.core.segment.SegmentFactory;
import krati.store.DataStore;
import krati.store.index.HashIndex;
import krati.store.index.Index;
import krati.util.IndexedIterator;
import krati.util.Numbers;

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
    
    /**
     * System change number is not volatile for it is used by synchronized write only.
     */
    private long _scn;
    
    /**
     * The Persistable event listener, default <code>null</code>.
     */
    private volatile PersistableListener _listener = null;
    
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
        config.validate();
        config.save();
        
        _homeDir = config.getHomeDir();
        
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
        _scn = _bytesDB.getHWMark();
        
        // Create hash index
        _indexHome = new File(_homeDir, "index");
        int indexInitialCapacity =
                config.getInt(StoreParams.PARAM_INDEX_INITIAL_CAPACITY,
                              config.getInitialCapacity());
        int indexSegmentFileSizeMB =
                config.getInt(StoreParams.PARAM_INDEX_SEGMENT_FILE_SIZE_MB,
                              StoreParams.INDEX_SEGMENT_FILE_SIZE_MB_DEFAULT);
        double indexSegmentCompactFactor =
                config.getDouble(StoreParams.PARAM_INDEX_SEGMENT_COMPACT_FACTOR,
                                 config.getSegmentCompactFactor());
        SegmentFactory indexSegmentFactory =
                config.getClass(StoreParams.PARAM_INDEX_SEGMENT_FACTORY_CLASS, MemorySegmentFactory.class)
                .asSubclass(SegmentFactory.class).newInstance();
        
        StoreConfig indexConfig = new StoreConfig(_indexHome, indexInitialCapacity);
        indexConfig.setBatchSize(config.getBatchSize());
        indexConfig.setNumSyncBatches(config.getNumSyncBatches());
        indexConfig.setIndexesCached(true);                         // indexes.dat is always cached
        indexConfig.setSegmentFactory(indexSegmentFactory);
        indexConfig.setSegmentFileSizeMB(indexSegmentFileSizeMB);
        indexConfig.setSegmentCompactFactor(indexSegmentCompactFactor);
        indexConfig.setHashLoadFactor(config.getHashLoadFactor());
        indexConfig.setHashFunction(config.getHashFunction());
        indexConfig.setDataHandler(config.getDataHandler());
        _index = new HashIndex(indexConfig);
        initIndexPersistableListener();
        
        _logger.info("opened indexHome=" + _indexHome.getAbsolutePath() + " storeHome=" + _storeHome.getAbsolutePath());
    }
    
    /**
     * Creates a new IndexedDataStore instance.
     * The created store has the following parameters:
     * 
     * <pre>
     *   BytesDB segmentCompactFactor : 0.5
     *   Index segmentCompactFactor   : 0.5
     *   Index hashLoadFactor         : 0.75
     *   Index hashFunction           : krati.util.FnvHashFunction
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
        
        // Create bytesDB
        _storeHome = new File(homeDir, "store");
        
        int storeInitialCapacity = initialCapacity;
        StoreConfig storeConfig = new StoreConfig(_storeHome, storeInitialCapacity);
        storeConfig.setBatchSize(batchSize);
        storeConfig.setNumSyncBatches(numSyncBatches);
        storeConfig.setSegmentFileSizeMB(storeSegmentFileSizeMB);
        storeConfig.setSegmentFactory(storeSegmentFactory);
        _bytesDB = new BytesDB(storeConfig);
        _scn = _bytesDB.getHWMark();
        
        // Create index
        _indexHome = new File(homeDir, "index");
        
        int indexInitialCapacity = initialCapacity;
        StoreConfig indexConfig = new StoreConfig(_indexHome, indexInitialCapacity);
        indexConfig.setBatchSize(batchSize);
        indexConfig.setNumSyncBatches(numSyncBatches);
        indexConfig.setSegmentFileSizeMB(indexSegmentFileSizeMB);
        indexConfig.setSegmentFactory(indexSegmentFactory);
        _index = new HashIndex(indexConfig);
        initIndexPersistableListener();
        
        _logger.info("opened indexHome=" + _indexHome.getAbsolutePath() + " storeHome=" + _storeHome.getAbsolutePath());
    }
    
    /**
     * Creates a new IndexedDataStore instance.
     * The created store has the following parameters:
     * 
     * <pre>
     *   BytesDB segmentCompactFactor : 0.5
     *   Index segmentCompactFactor   : 0.5
     *   Index hashLoadFactor         : 0.75
     *   Index hashFunction           : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir                - the home directory of IndexedDataStore
     * @param batchSize              - the number of updates per update batch
     * @param numSyncBatches         - the number of update batches required for updating <code>indexes.dat</code>
     * @param indexInitLevel         - the level for initializing HashIndex
     * @param indexSegmentFileSizeMB - the segment file size in MB for HashIndex
     * @param indexSegmentFactory    - the segment factory for HashIndex
     * @param storeInitLevel         - the level for initializing BytesDB
     * @param storeSegmentFileSizeMB - the segment file size in MB for BytesDB
     * @param storeSegmentFactory    - the segment factory for BytesDB
     * @throws Exception if this IndexedDataStore cannot be created.
     */
    public IndexedDataStore(File homeDir,
                            int batchSize,
                            int numSyncBatches,
                            int indexInitLevel,
                            int indexSegmentFileSizeMB,
                            SegmentFactory indexSegmentFactory,
                            int storeInitLevel,
                            int storeSegmentFileSizeMB,
                            SegmentFactory storeSegmentFactory) throws Exception {
        this._homeDir = homeDir;
        
        // Create bytesDB
        _storeHome = new File(homeDir, "store");
        
        int storeInitialCapacity = StoreParams.getDynamicStoreInitialCapacity(storeInitLevel);
        StoreConfig storeConfig = new StoreConfig(_storeHome, storeInitialCapacity);
        storeConfig.setBatchSize(batchSize);
        storeConfig.setNumSyncBatches(numSyncBatches);
        storeConfig.setSegmentFileSizeMB(storeSegmentFileSizeMB);
        storeConfig.setSegmentFactory(storeSegmentFactory);
        _bytesDB = new BytesDB(storeConfig);
        _scn = _bytesDB.getHWMark();
        
        // Create index
        _indexHome = new File(homeDir, "index");
        
        int indexInitialCapacity = StoreParams.getDynamicStoreInitialCapacity(indexInitLevel);
        StoreConfig indexConfig = new StoreConfig(_indexHome, indexInitialCapacity);
        indexConfig.setBatchSize(batchSize);
        indexConfig.setNumSyncBatches(numSyncBatches);
        indexConfig.setSegmentFileSizeMB(indexSegmentFileSizeMB);
        indexConfig.setSegmentFactory(indexSegmentFactory);
        _index = new HashIndex(indexConfig);
        initIndexPersistableListener();
        
        _logger.info("opened indexHome=" + _indexHome.getAbsolutePath() + " storeHome=" + _storeHome.getAbsolutePath());
    }
    
    /**
     * Initialize the Index persistable listener.
     */
    protected void initIndexPersistableListener() {
        _index.setPersistableListener(new PersistableListener() {
            @Override
            public void beforePersist() {
                try {
                    PersistableListener l = _listener;
                    if(l != null) l.beforePersist();
                } catch (Exception e) {
                    _logger.error("failed on calling beforePersist", e);
                }
                
                try {
                    _bytesDB.persist();
                } catch (Exception e) {
                    _logger.error("failed on calling beforePersist", e);
                }
            }
            
            @Override
            public void afterPersist() {
                try {
                    PersistableListener l = _listener;
                    if(l != null) l.afterPersist();
                } catch(Exception e) {
                    _logger.error("failed on calling afterPersist", e);
                }
            }
        });
    }
    
    /**
     * Gets the next system change number.
     */
    protected long nextScn() {
        return ++_scn;
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
    
    /**
     * Gets the underlying DB index associated with the specified <code>key</code>.
     * 
     * @param key - the key
     * @return <code>-1</code> if the specified <code>key</code> is <code>null</code> or not found.
     */
    public final int getDBIndex(byte[] key) {
        if(key == null) return -1;
        
        byte[] metaBytes = _index.lookup(key);
        if(metaBytes == null) return -1;
        
        IndexMeta meta = IndexMeta.parse(metaBytes);
        if(meta == null) return -1;
        
        return meta.getDataAddr();
    }
    
    @Override
    public int getLength(byte[] key) {
        if(key == null) return -1;
        
        byte[] metaBytes = _index.lookup(key);
        if(metaBytes == null) return -1;
        
        IndexMeta meta = IndexMeta.parse(metaBytes);
        if(meta == null) return -1;
        
        return _bytesDB.getLength(meta.getDataAddr());
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
            int index = _bytesDB.add(value, nextScn());
            metaBytes = IndexMeta.build(index);
            
            // Update hashIndex
            _index.update(key, metaBytes);
        } else {
            // Update bytes DB
            int index = meta.getDataAddr();
            _bytesDB.set(index, value, nextScn());
            
            // No need to update hashIndex
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
        _bytesDB.set(meta.getDataAddr(), null, nextScn());
        
        // Update index 
        _index.update(key, null);
        
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
            byte[] bytes = new byte[META_SIZE];
            Numbers.intBytesBE(dataAddr, bytes);
            return bytes;
        }
        
        static IndexMeta parse(byte[] metaBytes) {
            if(metaBytes.length != META_SIZE) return null;
            int dataAddr = Numbers.intValueBE(metaBytes);
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
            _index.close();
            _bytesDB.close();
        } catch(IOException ioe) {
            _bytesDB.close();
            throw ioe;
        }
    }
    
    /**
     * Gets the persistable event listener.
     */
    public final PersistableListener getPersistableListener() {
        return _listener;
    }
    
    /**
     * Sets the persistable event listener.
     * 
     * @param listener
     */
    public final void setPersistableListener(PersistableListener listener) {
        this._listener = listener;
    }
}
