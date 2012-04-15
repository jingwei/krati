/*
 * Copyright (c) 2011 LinkedIn, Inc
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

package test.store.avro;

import java.io.File;
import java.nio.ByteOrder;

import org.apache.avro.Schema;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.io.Serializer;
import krati.io.serializer.IntSerializer;
import krati.store.avro.SimpleAvroArray;
import krati.store.avro.AvroStore;

/**
 * TestAvroArrayJoiner
 * 
 * @author jwu
 * @since 09/26, 2011
 */
public class TestAvroArrayJoiner extends AbstractTestAvroStoreJoiner<Integer> {
    
    @Override
    protected Integer createKey(int memberId) {
        return memberId;
    }
    
    @Override
    protected Serializer<Integer> createKeySerializer() {
        return new IntSerializer(ByteOrder.BIG_ENDIAN);
    }
    
    @Override
    protected AvroStore<Integer> createAvroStore(File storeDir, int capacity, Schema schema, Serializer<Integer> keySerializer) throws Exception {
        StoreConfig config = new StoreConfig(storeDir, capacity);
        config.setSegmentFactory(createSegmentFactory());
        config.setSegmentFileSizeMB(32);
        config.setNumSyncBatches(2);
        config.setBatchSize(100);
        
        return new SimpleAvroArray(
                StoreFactory.createStaticArrayStore(config),
                schema);
    }
}
