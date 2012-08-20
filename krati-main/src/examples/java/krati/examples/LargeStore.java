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

package krati.examples;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.StoreParams;
import krati.core.segment.WriteBufferSegmentFactory;
import krati.io.Closeable;
import krati.store.DataStore;
import krati.store.index.HashIndexDataHandler;

/**
 * This class provides a template for creating a very-large key-value store.
 * 
 * <ul>
 *  <li>The store has hundreds of millions of keys. </li>
 *  <li>The size of keys is significantly smaller than the size of values. </li>
 * </ul>
 * 
 * <p>
 * For example, the store has 200,000,000 keys; the size of keys is 8 to 16 bytes;
 * the size of values is larger than 128 bytes.
 * </p>
 * 
 * @author jwu
 * @since 08/15, 2012
 */
public class LargeStore implements Closeable {
    private final int _initialCapacity;
    private final DataStore<byte[], byte[]> _store;
    
    /**
     * Constructs a new instance of LargeStore.
     * 
     * @param homeDir         - the home directory of LargeStore.
     * @param initialCapacity - the initial capacity of LargeStore, which is expected to be 8 to 16 times smaller than the expected number of keys.
     * <ul>
     * <li> This value should be significantly (e.g., 8 to 16 times) smaller than the expected number of keys. </li>
     * <li> This value should NOT be modified once the underlying store is created. </li>
     * </ul>
     * @throws Exception if a LargeStore instance can not be created.
     */
    public LargeStore(File homeDir, int initialCapacity) throws Exception {
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
     */
    protected DataStore<byte[], byte[]> createDataStore(File homeDir, int initialCapacity) throws Exception {
        StoreConfig config = new StoreConfig(homeDir, initialCapacity);
        
        config.setBatchSize(10000);
        config.setNumSyncBatches(100);
        
        // Configure store segments
        config.setSegmentFactory(new WriteBufferSegmentFactory());
        config.setSegmentFileSizeMB(128);
        config.setSegmentCompactFactor(0.67);
        
        // Configure index segments
        config.setInt(StoreParams.PARAM_INDEX_SEGMENT_FILE_SIZE_MB, 32);
        config.setDouble(StoreParams.PARAM_INDEX_SEGMENT_COMPACT_FACTOR, 0.5);
        
        // Configure to reduce memory footprint
        config.setDataHandler(new HashIndexDataHandler());
        
        // Disable linear hashing
        config.setHashLoadFactor(1.0);
        
        return StoreFactory.createIndexedDataStore(config);
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
     * Checks if the <code>LargeStore</code> is open for operations.
     */
    @Override
    public boolean isOpen() {
        return _store.isOpen();
    }
    
    /**
     * Opens the store.
     * 
     * @throws IOException
     */
    @Override
    public void open() throws IOException {
        _store.open();
    }
    
    /**
     * Closes the store.
     * 
     * @throws IOException
     */
    public void close() throws IOException {
        _store.close();
    }
    
    /**
     * Run this example using the command below:
     * 
     * <pre>
     * java -server -Xms12G -Xmx12G -XX:+UseConcMarkSweepGC krati.examples.LargeStore homeDir initialCapacity
     * </pre>
     * 
     * <p>
     * The Java JVM size can be calculated based on the expected number of keys, <code>N</code>, using the following equation:
     * <pre>
     *   N * (2 * keySize + 32) / 1024 / 1024 / 1024 plus 4
     * </pre>
     * 
     * <p>
     * For example, given that the expected number of keys is 200,000,000 and the size of keys is 10 bytes,
     * the required Java JVM size is approximately
     * <pre>
     *   200000000 * (2 * 10 + 32) / 1024 / 1024 / 1024 + 4 = 14G
     * </pre>
     * 
     * <p>
     * The <code>initialCapacity</code> can be calculated using the following:
     * <pre>
     *   N/16 or N/8
     * </pre>
     */
    public static void main(String[] args) {
        try {
            // Parse arguments: homeDir initialCapacity
            File homeDir = new File(args[0]);
            int initialCapacity = Integer.parseInt(args[1]);
            
            // Create an instance of LargeStore
            LargeStore store = new LargeStore(homeDir, initialCapacity);
            
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
