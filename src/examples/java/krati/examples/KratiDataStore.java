package krati.examples;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.segment.MemorySegmentFactory;
import krati.store.DataStore;

/**
 * Sample code for Krati DataStore.
 * 
 * @author jwu
 * 
 */
public class KratiDataStore {
    private final int _initialCapacity;
    private final DataStore<byte[], byte[]> _store;
    
    /**
     * Constructs KratiDataStore.
     * 
     * @param homeDir           the home directory of DataStore.
     * @param initialCapacity   the initial capacity of DataStore.
     * @throws Exception if a DataStore instance can not be created.
     */
    public KratiDataStore(File homeDir, int initialCapacity) throws Exception {
        _initialCapacity = initialCapacity;
        _store = createDataStore(homeDir, initialCapacity);
    }
    
    /**
     * @return the underlying data store.
     */
    public final DataStore<byte[], byte[]> getDataStore() {
        return _store;
    }
    
    /**
     * Creates a DataStore instance.
     * <p>
     * Subclasses can override this method to provide a specific DataStore implementation
     * such as DynamicDataStore and IndexedDataStore or provide a specific SegmentFactory
     * such as ChannelSegmentFactory, MappedSegmentFactory and WriteBufferSegment.
     */
    protected DataStore<byte[], byte[]> createDataStore(File homeDir, int initialCapacity) throws Exception {
        StoreConfig config = new StoreConfig(homeDir, initialCapacity);
        config.setSegmentFactory(new MemorySegmentFactory());
        config.setSegmentFileSizeMB(64);
        
        return StoreFactory.createStaticDataStore(config);
    }
    
    /**
     * Creates data for a given key.
     * Subclasses can override this method to provide specific values for a given key.
     */
    protected byte[] createDataForKey(String key) {
        return ("Here is your data for " + key).getBytes();
    }
    
    /**
     * Populates the underlying data store.
     * 
     * @throws Exception
     */
    public void populate() throws Exception {
        for (int i = 0; i < _initialCapacity; i++) {
            String str = "key." + i;
            byte[] key = str.getBytes();
            byte[] value = createDataForKey(str);
            _store.put(key, value);
        }
        _store.sync();
    }
    
    /**
     * Perform a number of random reads from the underlying data store.
     * 
     * @param readCnt the number of reads
     */
    public void doRandomReads(int readCnt) {
        Random rand = new Random();
        for (int i = 0; i < readCnt; i++) {
            int keyId = rand.nextInt(_initialCapacity);
            String str = "key." + keyId;
            byte[] key = str.getBytes();
            byte[] value = _store.get(key);
            System.out.printf("Key=%s\tValue=%s%n", str, new String(value));
        }
    }
    
    /**
     * Close the underlying store.
     * 
     * @throws IOException
     */
    public void close() throws IOException {
        _store.close();
    }
    
    /**
     * java -Xmx4G krati.examples.KratiDataStore homeDir initialCapacity 
     */
    public static void main(String[] args) {
        try {
            // Parse arguments: keyCount homeDir
            File homeDir = new File(args[0]);
            int initialCapacity = Integer.parseInt(args[1]);
            
            // Create an instance of Krati DataStore
            KratiDataStore store = new KratiDataStore(homeDir, initialCapacity);
            
            // Populate data store
            store.populate();
            
            // Perform some random reads from data store.
            store.doRandomReads(10);
            
            // Close data store
            store.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
