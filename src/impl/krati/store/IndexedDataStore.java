package krati.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

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
 */
public class IndexedDataStore implements DataStore<byte[], byte[]> {
    private final static Logger _logger = Logger.getLogger(IndexedDataStore.class);
    private final BytesDB _bytesDB;
    private final Index _index;
    private final File _indexHome;
    private final File _storeHome;
    
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
        _storeHome = new File(homeDir, "store");
        _bytesDB = new BytesDB(_storeHome,
                               storeInitLevel,
                               batchSize,
                               numSyncBatches,
                               storeSegmentFileSizeMB,
                               storeSegmentFactory);
        
        _indexHome = new File(homeDir, "index");
        _index = new HashIndex(_indexHome,
                               indexInitLevel,
                               batchSize,
                               numSyncBatches,
                               indexSegmentFileSizeMB,
                               indexSegmentFactory);
        
        _logger.info("opened indexHome=" + _indexHome.getAbsolutePath() + " storeHome=" + _storeHome.getAbsolutePath());
    }
    
    public final File getIndexHome() {
        return _indexHome;
    }
    
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
    public boolean put(byte[] key, byte[] value) throws Exception {
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
        
        return true;
    }
    
    @Override
    public boolean delete(byte[] key) throws Exception {
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
        
        return true;
    }
    
    @Override
    public void clear() throws IOException {
        _bytesDB.clear();
        _index.clear();
    }
    
    @Override
    public void persist() throws IOException {
        _bytesDB.persist();
        _index.persist();
    }
    
    @Override
    public void sync() throws IOException {
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
}
