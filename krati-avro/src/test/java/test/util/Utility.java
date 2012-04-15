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

package test.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.segment.MemorySegmentFactory;
import krati.store.DataStore;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

/**
 * Utility
 * 
 * @author jwu
 * @since 10/03, 2011
 */
public class Utility {
    
    public static DataStore<byte[], byte[]> createStaticDataStore(File storeDir, int capacity) throws Exception {
        StoreConfig config = new StoreConfig(storeDir, capacity);
        config.setSegmentFactory(new MemorySegmentFactory());
        config.setSegmentFileSizeMB(32);
        config.setNumSyncBatches(2);
        config.setBatchSize(100);
        
        return StoreFactory.createStaticDataStore(config);
    }
    
    public static DataStore<byte[], byte[]> createDynamicDataStore(File storeDir, int capacity) throws Exception {
        StoreConfig config = new StoreConfig(storeDir, capacity);
        config.setSegmentFactory(new MemorySegmentFactory());
        config.setSegmentFileSizeMB(32);
        config.setNumSyncBatches(2);
        config.setBatchSize(100);
        
        return StoreFactory.createDynamicDataStore(config);
    }
    
    public static Schema createPersonSchema() {
        List<Field> fields = new ArrayList<Field>();
        fields.add(new Field("id", Schema.create(Type.INT), null, null));
        fields.add(new Field("age", Schema.create(Type.INT), null, null));
        fields.add(new Field("fname", Schema.create(Type.STRING), null, null));
        fields.add(new Field("lname", Schema.create(Type.STRING), null, null));
        
        Schema schema = Schema.createRecord("Person", null, "avro.test", false);
        schema.setFields(fields);
        
        return schema;
    }
    
    public static Schema createAddressSchema() {
        List<Field> fields = new ArrayList<Field>();
        fields.add(new Field("id", Schema.create(Type.INT), null, null));
        fields.add(new Field("city", Schema.create(Type.STRING), null, null));
        fields.add(new Field("state", Schema.create(Type.STRING), null, null));
        fields.add(new Field("country", Schema.create(Type.STRING), null, null));
        fields.add(new Field("postal_code", Schema.create(Type.STRING), null, null));
        
        Schema schema = Schema.createRecord("Address", null, "avro.test", false);
        schema.setFields(fields);
        
        return schema;
    }
}
