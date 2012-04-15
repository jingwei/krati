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

import junit.framework.TestCase;
import krati.core.StoreConfig;
import krati.core.StorePartitionConfig;
import krati.core.segment.MappedSegmentFactory;
import krati.store.ArrayStore;
import krati.store.DynamicDataArray;
import krati.store.StaticArrayStorePartition;
import krati.store.StaticDataArray;
import krati.store.factory.ArrayStoreFactory;
import krati.store.factory.DynamicArrayStoreFactory;
import krati.store.factory.StaticArrayStoreFactory;
import test.util.FileUtils;

/**
 * TestArrayStoreFactory
 * 
 * @author jwu
 * @since 12/06, 2011
 */
public class TestArrayStoreFactory  extends TestCase {
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
            } finally {
                _config = null;
            }
        }
    }
    
    protected void setUpStorePartitionConfig() {
        tearDown();
        
        try {
            _config = new StorePartitionConfig(FileUtils.getTestDir(getClass()), 100000, 100);
            _config.setBatchSize(1000);
            _config.setNumSyncBatches(5);
            _config.setSegmentFileSizeMB(8);
            _config.setSegmentFactory(new MappedSegmentFactory());
        } catch (IOException e) {
            e.printStackTrace();
            _config = null;
        }
    }
    
    public void testStaticArrayStoreFactory() throws IOException {
        ArrayStoreFactory storeFactory = new StaticArrayStoreFactory(); 
        ArrayStore store = storeFactory.create(_config);
        assertEquals(StaticDataArray.class, store.getClass());
        store.close();
    }
    
    public void testDynamicDataStoreFactory() throws IOException {
        ArrayStoreFactory storeFactory = new DynamicArrayStoreFactory(); 
        ArrayStore store = storeFactory.create(_config);
        assertEquals(DynamicDataArray.class, store.getClass());
        store.close();
    }
    
    public void testStaticArrayStorePartitionFactory() throws IOException {
        setUpStorePartitionConfig();
        
        ArrayStoreFactory storeFactory = new StaticArrayStoreFactory(); 
        ArrayStore store = storeFactory.create(_config);
        assertEquals(StaticArrayStorePartition.class, store.getClass());
        
        StaticArrayStorePartition p = (StaticArrayStorePartition)store;
        assertEquals(p.capacity(), p.length());
        assertEquals(p.capacity(), store.capacity());
        assertEquals(p.capacity(), _config.getInitialCapacity());
        assertEquals(p.getIndexStart(), ((StorePartitionConfig)_config).getPartitionStart());
        
        store.close();
    }
}
