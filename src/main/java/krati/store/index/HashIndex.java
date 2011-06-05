package krati.store.index;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import krati.core.segment.SegmentFactory;
import krati.store.DynamicDataStore;
import krati.util.FnvHashFunction;
import krati.util.HashFunction;

/**
 * HashIndex is for serving index lookup from main memory and has the
 * best performance when {@link krati.core.segment.MemorySegmentFactory} is used to store
 * indexes in memory.
 * 
 * @author jwu
 * 
 * 06/04, 2011 - Added support for Closeable
 */
public class HashIndex implements Index {
    private final static Logger _logger = Logger.getLogger(HashIndex.class);
    private final DynamicDataStore _store;
    
    public HashIndex(File homeDir, SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             6,     /* initLevel */
             1000,  /* batchSize */
             5,     /* numSyncBatches */
             32,    /* segmentFileSizeMB */
             segmentFactory,
             new FnvHashFunction());
    }
    
    public HashIndex(File homeDir,
                     int initLevel,
                     int batchSize,
                     int numSyncBatches,
                     SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             initLevel,
             batchSize,
             numSyncBatches,
             32,    /* segmentFileSizeMB */
             segmentFactory,
             new FnvHashFunction());
    }
    
    public HashIndex(File homeDir,
                     int initLevel,
                     int batchSize,
                     int numSyncBatches,
                     int segmentFileSizeMB,
                     SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             initLevel,
             batchSize,
             numSyncBatches,
             segmentFileSizeMB,
             segmentFactory,
             new FnvHashFunction());
    }
    
    public HashIndex(File homeDir,
                     int initLevel,
                     int batchSize,
                     int numSyncBatches,
                     int segmentFileSizeMB,
                     SegmentFactory segmentFactory,
                     HashFunction<byte[]> hashFunction) throws Exception {
        _logger.info("init " + homeDir.getPath());
        _store = new DynamicDataStore(
                homeDir,
                initLevel,
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                0.5,   /* segmentCompactFactor */
                0.75,  /* store hashHoadFactor */
                hashFunction);
        _logger.info("init done");
    }
    
    @Override
    public void sync() throws IOException {
        _store.sync();
    }
    
    @Override
    public void persist() throws IOException {
        _store.persist();
    }
    
    @Override
    public void clear() throws IOException {
        _store.clear();
    }
    
    @Override
    public byte[] lookup(byte[] keyBytes) {
        return _store.get(keyBytes);
    }
    
    @Override
    public void update(byte[] keyBytes, byte[] metaBytes) throws Exception {
        _store.put(keyBytes, metaBytes);
    }
    
    @Override
    public Iterator<byte[]> keyIterator() {
        return _store.keyIterator();
    }
    
    @Override
    public Iterator<Entry<byte[], byte[]>> iterator() {
        return _store.iterator();
    }
    
    @Override
    public boolean isOpen() {
        return _store.isOpen();
    }
    
    @Override
    public void open() throws IOException {
        _store.open();
    }
    
    @Override
    public void close() throws IOException {
        _store.close();
    }
}
