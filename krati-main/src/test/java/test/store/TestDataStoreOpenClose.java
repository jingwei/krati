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

package test.store;

import java.io.File;
import java.util.Random;

import test.util.DirUtils;

import junit.framework.TestCase;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.segment.MemorySegmentFactory;
import krati.store.DataStore;
import krati.util.IndexedIterator;

/**
 * TestDataStoreOpenClose
 * 
 * @author jwu
 * @since 06/10, 2012
 */
public class TestDataStoreOpenClose extends TestCase {
    private Random _rand = new Random();
    private DataStore<byte[], byte[]> _store;
    
    @Override
    protected void setUp() {
        try {
            File homeDir = DirUtils.getTestDir(TestDataStoreOpenClose.class);
            int initialCapacity = 1000 + _rand.nextInt(1000);
            _store = createDataStore(homeDir, initialCapacity);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            if(_store != null) {
                _store.close();
            }
            
            File homeDir = DirUtils.getTestDir(TestDataStoreOpenClose.class);
            DirUtils.deleteDirectory(homeDir);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    protected DataStore<byte[], byte[]> createDataStore(File homeDir, int initialCapacity) throws Exception {
        StoreConfig config = new StoreConfig(homeDir, initialCapacity);
        config.setSegmentFactory(new MemorySegmentFactory());
        config.setSegmentFileSizeMB(32);
        
        return StoreFactory.createIndexedDataStore(config);
    }
    
    /**
     * Creates data for a given key.
     */
    protected byte[] createDataForKey(String key) {
        return ("Here is your data for " + key).getBytes();
    }
    
    /**
     * Populates the underlying data store.
     */
    protected void populate(int num) throws Exception {
        for (int i = 0; i < num; i++) {
            String str = "key." + i;
            byte[] key = str.getBytes();
            byte[] value = createDataForKey(str);
            _store.put(key, value);
        }
        _store.sync();
    }
    
    /**
     * Check if the number of keys match the expected number.
     */
    protected void count(int expected) {
        int count = 0;
        IndexedIterator<byte[]> itr = _store.keyIterator();
        
        while(itr.hasNext()) {
            itr.next();
            count++;
        }
        
        assertEquals(expected, count);
    }
    
    /**
     * Test the number of keys upon opening/closing a data store.
     */
    public void testOpenClose() throws Exception {
        int num;
        
        // Populate data store
        num = 1000 + _rand.nextInt(1000);
        populate(num);
        count(num);
        
        num += 30000 + _rand.nextInt(10000);
        populate(num);
        count(num);
        
        _store.close();
        _store.open();
        count(num);
        
        num += 300000 + _rand.nextInt(100000);
        populate(num);
        count(num);
        
        _store.close();
        _store.open();
        count(num);
        
        _store.close();
    }
}
