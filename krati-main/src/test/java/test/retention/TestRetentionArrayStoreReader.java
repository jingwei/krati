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

package test.retention;

import java.io.File;
import java.util.UUID;

import krati.core.StoreConfig;
import krati.core.StorePartitionConfig;
import krati.io.Serializer;
import krati.io.serializer.IntSerializer;
import krati.io.serializer.StringSerializerUtf8;
import krati.retention.Retention;
import krati.retention.RetentionConfig;
import krati.retention.SimpleRetention;
import krati.store.DataStore;
import krati.store.factory.ObjectStoreFactory;
import krati.store.factory.StaticObjectArrayFactory;

/**
 * TestRetentionArrayStoreReader
 * 
 * @author jwu
 * @since 02/22, 2012
 */
public class TestRetentionArrayStoreReader extends AbstractTestRetentionStoreReader<Integer, String> {
    private int _indexStart = _rand.nextInt(100) * 100000;
    private int _indexCount = 100000;
    
    @Override
    protected Serializer<Integer> createEventValueSerializer() {
        return new IntSerializer();
    }
    
    @Override
    protected Retention<Integer> createRetention() throws Exception {
        RetentionConfig<Integer> config = new RetentionConfig<Integer>(getId(), getHomeDir());
        config.setBatchSize(getEventBatchSize());
        config.setRetentionPolicy(createRetentionPolicy());
        config.setEventValueSerializer(createEventValueSerializer());
        config.setEventClockSerializer(createEventClockSerializer());
        config.setRetentionSegmentFileSizeMB(16);
        
        return new SimpleRetention<Integer>(config);
    }
    
    @Override
    protected Integer nextKey() {
        return _indexStart + _rand.nextInt(_indexCount);
    }
    
    @Override
    protected String nextValue() {
        return "value." + UUID.randomUUID().toString();
    }
    
    @Override
    protected boolean checkKeyEquality(Integer key1, Integer key2) {
        return key1.intValue() == key2.intValue();
    }
    
    @Override
    protected boolean checkValueEquality(String value1, String value2) {
        return value1.equals(value2);
    }
    
    @Override
    protected DataStore<Integer, String> createStore() throws Exception {
        StoreConfig config = new StorePartitionConfig(new File(getHomeDir(), "store"), _indexStart, _indexCount);
        config.setSegmentFileSizeMB(16);
        config.setNumSyncBatches(10);
        config.setBatchSize(100);
        
        ObjectStoreFactory<Integer, String> factory = new StaticObjectArrayFactory<String>();
        return factory.create(config, new IntSerializer(), new StringSerializerUtf8());
    }
}
