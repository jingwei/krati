package krati.examples;

import java.io.File;
import java.util.Random;

import krati.core.segment.SegmentFactory;
import krati.store.DataStore;
import krati.store.StaticDataStore;

/**
 * Sample code for Krati DataStore.
 * 
 * @author jwu
 *
 */
public class KratiDataStore {
    private final int _keyCount;
    private final DataStore<byte[], byte[]> _store;
    
    /**
     * Constructs KratiDataStore.
     * 
     * @param keyCount   the number of keys.
     * @param homeDir    the home directory for storing data.
     * @throws Exception if a DataStore instance can not be created.
     */
    public KratiDataStore(int keyCount, File homeDir) throws Exception {
        _keyCount = keyCount;
        _store = createDataStore(keyCount, homeDir);
    }
    
    /**
     * @return the underlying data store.
     */
    public final DataStore<byte[], byte[]> getDataStore() {
        return _store;
    }
    
    /**
     * Creates a data store instance.
     * Subclasses can override this method to provide a specific DataStore implementation
     * such as DynamicDataStore and IndexedDataStore.
     */
    protected DataStore<byte[], byte[]> createDataStore(int keyCount, File storeDir) throws Exception {
        int capacity = (int)(keyCount * 1.5);
        return new StaticDataStore(storeDir,
                                   capacity, /* capacity */
                                   10000,    /* update batch size */
                                   5,        /* number of update batches required to sync indexes.dat */
                                   128,      /* segment file size in MB */
                                   createSegmentFactory());
    }
    
    /**
     * Creates a segment factory.
     * Subclasses can override this method to provide a specific segment factory
     * such as ChannelSegmentFactory and MappedSegmentFactory.
     * 
     * @return the segment factory. 
     */
    protected SegmentFactory createSegmentFactory() {
        return new krati.core.segment.MemorySegmentFactory();
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
        for (int i = 0; i < _keyCount; i++) {
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
            int keyId = rand.nextInt(_keyCount);
            String str = "key." + keyId;
            byte[] key = str.getBytes();
            byte[] value = _store.get(key);
            System.out.printf("Key=%s\tValue=%s%n", str, new String(value));
        }
    }
    
    /**
     * java -Xmx4G krati.examples.KratiDataStore keyCount homeDir
     */
    public static void main(String[] args) {
        try {
            // Parse arguments: keyCount homeDir
            int keyCount = Integer.parseInt(args[0]);
            File homeDir = new File(args[1]);
            
            // Create an instance of Krati DataStore
            File storeHomeDir = new File(homeDir, KratiDataStore.class.getSimpleName());
            KratiDataStore store = new KratiDataStore(keyCount, storeHomeDir);
            
            // Populate data store
            store.populate();
            
            // Perform some random reads from data store.
            store.doRandomReads(10);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
