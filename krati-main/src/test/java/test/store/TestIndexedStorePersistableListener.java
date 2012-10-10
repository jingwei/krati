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
import java.io.IOException;
import java.util.Random;

import test.util.DirUtils;
import test.util.PersistCounter;
import junit.framework.TestCase;
import krati.core.StoreConfig;
import krati.core.StoreParams;
import krati.core.segment.MemorySegmentFactory;
import krati.core.segment.WriteBufferSegmentFactory;
import krati.store.IndexedDataStore;

/**
 * TestIndexedStorePersistableListener
 * 
 * @author jwu
 * @since 09/18, 2012
 */
public class TestIndexedStorePersistableListener extends TestCase {
    protected Random _rand = new Random();
    protected IndexedDataStore _store;
    
    protected int getBatchSize() {
        return 100;
    }
    
    @Override
    protected void setUp() {
        try {
            File storeDir = DirUtils.getTestDir(getClass());
            StoreConfig config = new StoreConfig(storeDir, 100000);
            config.setBatchSize(getBatchSize());
            config.setNumSyncBatches(10);
            config.setSegmentFileSizeMB(32);
            config.setSegmentFactory(new WriteBufferSegmentFactory());
            config.setInt(StoreParams.PARAM_INDEX_SEGMENT_FILE_SIZE_MB, 8);
            config.setClass(StoreParams.PARAM_SEGMENT_FACTORY_CLASS, MemorySegmentFactory.class);
            
            _store = new IndexedDataStore(config);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            File storeDir = DirUtils.getTestDir(getClass());
            DirUtils.deleteDirectory(storeDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    protected void nextPut() throws Exception {
        int num = _rand.nextInt();
        byte[] key = (num + ".key").getBytes();
        byte[] value = (num + ".value").getBytes();
        _store.put(key, value);
    }
    
    public void testPersistableListener() throws Exception {
        PersistCounter l = new PersistCounter();
        _store.setPersistableListener(l);
        
        assertEquals(0, l.getBeforeCount());
        assertEquals(0, l.getAfterCount());
        
        for(int i = 0, cnt = getBatchSize(); i < cnt; i++) {
            nextPut();
        }
        
        assertEquals(1, l.getBeforeCount());
        assertEquals(1, l.getAfterCount());
        
        for(int i = 0, cnt = getBatchSize(); i < cnt; i++) {
            nextPut();
        }
        
        assertEquals(2, l.getBeforeCount());
        assertEquals(2, l.getAfterCount());
        
        for(int i = 0, cnt = getBatchSize() + _rand.nextInt(10000); i < cnt; i++) {
            nextPut();
        }
        
        assertTrue(2 < l.getBeforeCount());
        assertTrue(2 < l.getAfterCount());
        assertEquals(l.getBeforeCount(), l.getAfterCount());
        System.out.println(String.format("persisted %d times", l.getAfterCount()));
    }
}
