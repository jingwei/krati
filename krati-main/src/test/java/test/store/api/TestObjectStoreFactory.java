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
import krati.io.serializer.IntSerializer;
import krati.io.serializer.StringSerializerUtf8;
import krati.store.DynamicDataArray;
import krati.store.DynamicDataStore;
import krati.store.IndexedDataStore;
import krati.store.ObjectStore;
import krati.store.SerializableObjectArray;
import krati.store.SerializableObjectStore;
import krati.store.StaticArrayStorePartition;
import krati.store.StaticDataArray;
import krati.store.StaticDataStore;
import krati.store.factory.DynamicObjectArrayFactory;
import krati.store.factory.DynamicObjectStoreFactory;
import krati.store.factory.IndexedObjectStoreFactory;
import krati.store.factory.ObjectStoreFactory;
import krati.store.factory.StaticObjectArrayFactory;
import krati.store.factory.StaticObjectStoreFactory;
import test.util.FileUtils;

/**
 * TestObjectStoreFactory
 * 
 * @author jwu
 * @since 12/06, 2011
 */
public class TestObjectStoreFactory extends TestCase {
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
    
    public void testStaticObjectArrayFactory1() throws IOException {
        ObjectStoreFactory<Integer, String> storeFactory = new StaticObjectArrayFactory<String>(); 
        ObjectStore<Integer, String> store = storeFactory.create(_config, new IntSerializer(), new StringSerializerUtf8());
        assertEquals(SerializableObjectArray.class, store.getClass());
        assertEquals(StaticDataArray.class, ((SerializableObjectArray<String>)store).getStore().getClass());
        store.close();
    }
    
    public void testStaticObjectArrayFactory2() throws IOException {
        setUpStorePartitionConfig();
        
        ObjectStoreFactory<Integer, String> storeFactory = new StaticObjectArrayFactory<String>(); 
        ObjectStore<Integer, String> store = storeFactory.create(_config, new IntSerializer(), new StringSerializerUtf8());
        assertEquals(SerializableObjectArray.class, store.getClass());
        assertEquals(StaticArrayStorePartition.class, ((SerializableObjectArray<String>)store).getStore().getClass());
        store.close();
    }
    
    public void testDynamicObjectArrayFactory() throws IOException {
        ObjectStoreFactory<Integer, String> storeFactory = new DynamicObjectArrayFactory<String>(); 
        ObjectStore<Integer, String> store = storeFactory.create(_config, new IntSerializer(), new StringSerializerUtf8());
        assertEquals(SerializableObjectArray.class, store.getClass());
        assertEquals(DynamicDataArray.class, ((SerializableObjectArray<String>)store).getStore().getClass());
        store.close();
    }
    
    public void testStaticObjectStoreFactory() throws IOException {
        ObjectStoreFactory<Integer, String> storeFactory = new StaticObjectStoreFactory<Integer, String>(); 
        ObjectStore<Integer, String> store = storeFactory.create(_config, new IntSerializer(), new StringSerializerUtf8());
        assertEquals(SerializableObjectStore.class, store.getClass());
        assertEquals(StaticDataStore.class, ((SerializableObjectStore<Integer, String>)store).getStore().getClass());
        store.close();
    }
    
    public void testDynamicObjectStoreFactory() throws IOException {
        ObjectStoreFactory<Integer, String> storeFactory = new DynamicObjectStoreFactory<Integer, String>(); 
        ObjectStore<Integer, String> store = storeFactory.create(_config, new IntSerializer(), new StringSerializerUtf8());
        assertEquals(SerializableObjectStore.class, store.getClass());
        assertEquals(DynamicDataStore.class, ((SerializableObjectStore<Integer, String>)store).getStore().getClass());
        store.close();
    }
    
    public void testIndexedObjectStoreFactory() throws IOException {
        ObjectStoreFactory<Integer, String> storeFactory = new IndexedObjectStoreFactory<Integer, String>(); 
        ObjectStore<Integer, String> store = storeFactory.create(_config, new IntSerializer(), new StringSerializerUtf8());
        assertEquals(SerializableObjectStore.class, store.getClass());
        assertEquals(IndexedDataStore.class, ((SerializableObjectStore<Integer, String>)store).getStore().getClass());
        store.close();
    }
}
