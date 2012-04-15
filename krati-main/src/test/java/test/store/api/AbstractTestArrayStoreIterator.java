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

package test.store.api;

import java.io.File;
import java.util.Arrays;
import java.util.Random;

import test.util.FileUtils;
import test.util.RandomBytes;

import junit.framework.TestCase;
import krati.store.ArrayStore;
import krati.store.ArrayStoreIndexIterator;
import krati.store.ArrayStoreIterator;

/**
 * AbstractTestArrayStoreIterator
 * 
 * @author jwu
 * @since 10/08, 2011
 */
public abstract class AbstractTestArrayStoreIterator  extends TestCase {
    protected File _homeDir;
    protected ArrayStore _store;
    protected Random _rand = new Random();
    
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
    
    protected int getRandomIndex() {
        return _store.getIndexStart() + _rand.nextInt(_store.capacity());
    }
    
    protected abstract ArrayStore createStore(File homeDir) throws Exception;
    
    public void testApiBasics() throws Exception {
        int randomIndex = getRandomIndex();
        byte[] randomBytes = RandomBytes.getBytes();
        
        for(int i = 0; i < 100; i++) {
            byte[] value = RandomBytes.getBytes();
            _store.set(getRandomIndex(), value, System.currentTimeMillis());
        }
        
        _store.set(randomIndex, randomBytes, System.currentTimeMillis());
        
        _store.sync();
        
        // Test ArrayStoreIndexIterator
        ArrayStoreIndexIterator iter1 = new ArrayStoreIndexIterator(_store);
        assertEquals(_store.getIndexStart(), iter1.index());
        assertTrue(iter1.hasNext());
        
        int cnt1 = 0;
        while(iter1.hasNext()) {
            iter1.next();
            cnt1++;
        }
        assertEquals(_store.capacity(), cnt1);
        
        for(int i = 0; i < 100; i++) {
            iter1.reset(getRandomIndex());
        }
        
        try {
            iter1.reset(_store.getIndexStart() - 1);
            assertTrue(false);
        } catch(ArrayIndexOutOfBoundsException e) {}
        
        try {
            iter1.reset(_store.getIndexStart() + _store.capacity());
            assertTrue(false);
        } catch(ArrayIndexOutOfBoundsException e) {}
        
        iter1.reset(randomIndex);
        assertTrue(iter1.hasNext());
        assertEquals(randomIndex, iter1.index());
        assertEquals(randomIndex, iter1.next().intValue());
        
        // Test ArrayStoreIndexIterator
        ArrayStoreIterator iter2 = new ArrayStoreIterator(_store);
        assertEquals(_store.getIndexStart(), iter2.index());
        assertTrue(iter2.hasNext());
        
        int cnt2 = 0;
        while(iter2.hasNext()) {
            iter2.next();
            cnt2++;
        }
        assertEquals(_store.capacity(), cnt2);
        
        for(int i = 0; i < 100; i++) {
            iter2.reset(getRandomIndex());
        }
        
        try {
            iter2.reset(_store.getIndexStart() - 1);
            assertTrue(false);
        } catch(ArrayIndexOutOfBoundsException e) {}
        
        try {
            iter2.reset(_store.getIndexStart() + _store.capacity());
            assertTrue(false);
        } catch(ArrayIndexOutOfBoundsException e) {}
        
        iter2.reset(randomIndex);
        assertTrue(iter2.hasNext());
        assertEquals(randomIndex, iter2.index());
        assertTrue(Arrays.equals(randomBytes, iter2.next().getValue()));
    }
}
