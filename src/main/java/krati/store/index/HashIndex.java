package krati.store.index;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import krati.core.StoreConfig;
import krati.core.StoreParams;
import krati.core.segment.SegmentFactory;
import krati.store.DynamicDataStore;
import krati.util.FnvHashFunction;

/**
 * HashIndex is for serving index lookup from main memory and has the
 * best performance when {@link krati.core.segment.MemorySegmentFactory MemorySegmentFactory}
 * is used to store indexes in memory.
 * 
 * @author jwu
 * 
 * <p>
 * 06/04, 2011 - Added support for Closeable
 * 06/28, 2011 - Added constructor using StoreConfig
 */
public class HashIndex implements Index {
    private final static Logger _logger = Logger.getLogger(HashIndex.class);
    private final DynamicDataStore _store;
    
    /**
     * Creates a new HashIndex instance.
     * 
     * @param config - HashIndex configuration
     * @throws Exception if the index cannot be created.
     */
    public HashIndex(StoreConfig config) throws Exception {
        _store = new DynamicDataStore(config);
        _logger.info("init " + config.getHomeDir().getPath());
    }
    
    /**
     * Creates a new HashIndex instance with the following parameters. 
     * 
     * <pre>
     *    segmentCompactFactor : 0.5
     *    Store hashLoadFactor : 0.75
     *    Store hashFunction   : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir           - the home directory of HashIndex
     * @param initLevel         - the level for initializing HashIndex
     * @param batchSize         - the number of updates per update batch
     * @param numSyncBatches    - the number of update batches required for updating <code>indexes.dat</code>
     * @param segmentFileSizeMB - the size of segment file in MB
     * @param segmentFactory    - the segment factory
     * @throws Exception if the index cannot be created.
     */
    public HashIndex(File homeDir,
                     int initLevel,
                     int batchSize,
                     int numSyncBatches,
                     int segmentFileSizeMB,
                     SegmentFactory segmentFactory) throws Exception {
        _store = new DynamicDataStore(
                homeDir,
                initLevel,
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT,
                StoreParams.HASH_LOAD_FACTOR_DEFAULT,
                new FnvHashFunction());
        _logger.info("init " + homeDir.getPath());
    }
    
    @Override
    public final int capacity() {
        return _store.capacity();
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
