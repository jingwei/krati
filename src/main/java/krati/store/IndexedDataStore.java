package krati.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import krati.core.segment.SegmentFactory;
import krati.store.DataStore;
import krati.store.index.HashIndex;
import krati.store.index.Index;

/**
 * IndexedDataStore.
 * 
 * @author jwu
 * 
 * <p>
 * 06/04, 2011 - Added support for Closeable
 * 06/04, 2011 - Added getHomeDir
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
    
    public IndexedDataStore(File homeDir,
                            int batchSize,
                            int numSyncBatches,
                            SegmentFactory indexSegmentFactory,
                            SegmentFactory storeSegmentFactory) throws Exception {
        this(homeDir,
             batchSize,
             numSyncBatches,
             8,
             32,
             indexSegmentFactory,
             8,
             256,
             storeSegmentFactory);
    }
    
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
        this._batchSize = batchSize;
        
        // Create bytesDB
        _storeHome = new File(homeDir, "store");
        _bytesDB = new BytesDB(_storeHome,
                               storeInitLevel,
                               batchSize,
                               numSyncBatches,
                               storeSegmentFileSizeMB,
                               storeSegmentFactory);
        
        // Create index
        _indexHome = new File(homeDir, "index");
        _index = new HashIndex(_indexHome,
                               indexInitLevel,
                               batchSize,
                               numSyncBatches,
                               indexSegmentFileSizeMB,
                               indexSegmentFactory);
        
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
    public Iterator<byte[]> keyIterator() {
        if(isOpen()) {
            return _index.keyIterator();
        }
        
        throw new StoreClosedException();
    }

    @Override
    public Iterator<Entry<byte[], byte[]>> iterator() {
        if(isOpen()) {
            return new IndexedDataStoreIterator(_index.iterator());
        }
        
        throw new StoreClosedException();
    }
    
    private class IndexedDataStoreIterator implements Iterator<Entry<byte[], byte[]>> {
        final Iterator<Entry<byte[], byte[]>> _indexIter;
        
        IndexedDataStoreIterator(Iterator<Entry<byte[], byte[]>> indexIter) {
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
