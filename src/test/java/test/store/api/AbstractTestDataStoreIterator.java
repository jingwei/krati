/*
 * Copyright (c) 2010-2011 LinkedIn, Inc
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

package test.store.api;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;
import java.util.Map.Entry;

import junit.framework.TestCase;
import krati.store.DataStore;
import krati.util.IndexedIterator;
import test.util.FileUtils;
import test.util.RandomBytes;

/**
 * AbstractTestDataStoreIterator
 * 
 * @author  jwu
 * @since   0.4.2
 * @version 0.4.2
 */
public abstract class AbstractTestDataStoreIterator extends TestCase {
    protected File _homeDir;
    protected DataStore<byte[], byte[]> _store;
    
    @Override
    protected void setUp() {
        try {
            _homeDir = FileUtils.getTestDir(getClass().getSimpleName());
            _store = createStore(_homeDir);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            _store.close();
            FileUtils.deleteDirectory(_homeDir);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    protected abstract DataStore<byte[], byte[]> createStore(File homeDir) throws Exception;
    
    public void testIterable() throws Exception {
        _store.clear();
        
        // Generate keys
        HashSet<String> keySet = new HashSet<String>(199);
        while(keySet.size() < 100) {
            keySet.add(UUID.randomUUID().toString());
        }
        assertEquals(100, keySet.size());
        
        // Populate store
        for(String key : keySet) {
            byte[] value = RandomBytes.getBytes();
            _store.put(key.getBytes(), value);
        }
        
        HashSet<String> keySet2 = new HashSet<String>(199);
        for(Entry<byte[], byte[]> e : _store) {
            keySet2.add(new String(e.getKey()));
        }
        
        assertEquals(keySet.size(), keySet2.size());
        
        keySet2.removeAll(keySet);
        assertEquals(0, keySet2.size());
    }
    
    public void testKeyIndexedIterator() throws Exception {
        IndexedIterator<byte[]> keyIter;
        
        _store.clear();
        keyIter = _store.keyIterator();
        assertFalse(keyIter.hasNext());
        
        // Generate keys
        HashSet<String> keySet = new HashSet<String>(199);
        while(keySet.size() < 100) {
            keySet.add(UUID.randomUUID().toString());
        }
        assertEquals(100, keySet.size());
        
        // Populate store
        for(String key : keySet) {
            byte[] value = RandomBytes.getBytes();
            _store.put(key.getBytes(), value);
        }
        
        // Check keys
        keyIter = _store.keyIterator();
        byte[] key1a = keyIter.next();
        
        keyIter.reset(0);
        byte[] key1b = keyIter.next();
        
        for(int i = 0; i < 10; i++) {
            keyIter.next();
        }
        keyIter.reset(keyIter.index());
        byte[] key1c = keyIter.next();
        
        assertTrue(Arrays.equals(key1a, key1b));
        
        // Re-open store
        _store.close();
        _store.open();
        
        // check keys
        keyIter = _store.keyIterator();
        byte[] key2a = keyIter.next();
        
        keyIter.reset(0);
        byte[] key2b = keyIter.next();

        for(int i = 0; i < 10; i++) {
            keyIter.next();
        }
        keyIter.reset(keyIter.index());
        byte[] key2c = keyIter.next();
        
        assertTrue(Arrays.equals(key1a, key2a));
        assertTrue(Arrays.equals(key1b, key2b));
        assertTrue(Arrays.equals(key1c, key2c));
    }
}