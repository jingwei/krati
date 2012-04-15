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

import java.io.IOException;

import test.util.FileUtils;
import junit.framework.TestCase;
import krati.core.StoreConfig;
import krati.core.segment.MappedSegmentFactory;
import krati.store.DataStore;
import krati.store.DynamicDataStore;
import krati.store.IndexedDataStore;
import krati.store.StaticDataStore;
import krati.store.factory.DataStoreFactory;
import krati.store.factory.DynamicDataStoreFactory;
import krati.store.factory.IndexedDataStoreFactory;
import krati.store.factory.StaticDataStoreFactory;

/**
 * TestDataStoreFactory
 * 
 * @author jwu
 * @since 12/06, 2011
 */
public class TestDataStoreFactory extends TestCase {
    private StoreConfig _config;
    
    @Override
    protected void setUp() {
        try {
            _config = new StoreConfig(FileUtils.getTestDir(getClass()), 100000);
            _config.setBatchSize(1000);
            _config.setNumSyncBatches(5);
            _config.setSegmentFileSizeMB(8);
            _config.setSegmentFactory(new MappedSegmentFactory());
        } catch (IOException e) {
            e.printStackTrace();
            _config = null;
        }
    }
    
    @Override
    protected void tearDown() {
        if(_config != null) {
            try {
                FileUtils.deleteDirectory(_config.getHomeDir());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    public void testStaticDataStoreFactory() throws IOException {
        DataStoreFactory storeFactory = new StaticDataStoreFactory(); 
        DataStore<byte[], byte[]> store = storeFactory.create(_config);
        assertEquals(StaticDataStore.class, store.getClass());
        store.close();
    }
    
    public void testDynamicDataStoreFactory() throws IOException {
        DataStoreFactory storeFactory = new DynamicDataStoreFactory(); 
        DataStore<byte[], byte[]> store = storeFactory.create(_config);
        assertEquals(DynamicDataStore.class, store.getClass());
        store.close();
    }
    
    public void testIndexedDataStoreFactory() throws IOException {
        DataStoreFactory storeFactory = new IndexedDataStoreFactory(); 
        DataStore<byte[], byte[]> store = storeFactory.create(_config);
        assertEquals(IndexedDataStore.class, store.getClass());
        store.close();
    }
}
