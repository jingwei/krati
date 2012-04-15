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

package test.store.avro;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;
import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.segment.SegmentFactory;
import krati.store.ArrayStore;
import krati.store.avro.SimpleAvroArray;
import krati.store.avro.AvroStore;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import test.util.DirUtils;

/**
 * TestAvroArray
 * 
 * @author jwu
 * @since 09/26, 2011
 */
public class TestAvroArray extends TestCase {
    protected Random _rand = new Random();
    protected AvroStore<Integer> _store;
    protected File _storeDir;
    
    protected SegmentFactory createSegmentFactory() {
        return new krati.core.segment.MemorySegmentFactory();
    }
    
    protected ArrayStore createArrayStore(File storeDir, int capacity) throws Exception {
        StoreConfig config = new StoreConfig(storeDir, capacity);
        config.setSegmentFactory(createSegmentFactory());
        config.setSegmentFileSizeMB(32);
        config.setNumSyncBatches(2);
        config.setBatchSize(100);
        
        return StoreFactory.createStaticArrayStore(config);
    }
    
    @Override
    protected void setUp() {
        try {
            File storeDir = DirUtils.getTestDir(getClass());
            _store = new SimpleAvroArray(
                    createArrayStore(storeDir, 1000),
                    createSchema());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            _store.close();
            DirUtils.deleteDirectory(DirUtils.getTestDir(getClass()));
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            _store = null;
        }
    }
    
    protected Schema createSchema() {
        List<Field> fields = new ArrayList<Field>();
        fields.add(new Field("age", Schema.create(Type.INT), null, null));
        fields.add(new Field("fname", Schema.create(Type.STRING), null, null));
        fields.add(new Field("lname", Schema.create(Type.STRING), null, null));
        
        Schema schema = Schema.createRecord("Person", null, "avro.test", false);
        schema.setFields(fields);
        
        return schema;
    }
    
    protected GenericRecord createRecord(Schema schema, int memberId) {
        GenericData.Record record = new GenericData.Record(schema);
        
        record.put("fname", new Utf8("firstName." + memberId));
        record.put("lname", new Utf8("lastName." + memberId));
        record.put("age", _rand.nextInt(100));
        
        return record;
    }
    
    public void testApiBasics() throws Exception {
        int key;
        GenericRecord value;
        HashMap<Integer, GenericRecord> map = new HashMap<Integer, GenericRecord>();
        
        for(int i = 0; i < 10; i++) {
            key = i;
            value = createRecord(_store.getSchema(), i);
            map.put(key, value);
            _store.put(key, value);
        }
        
        _store.sync();
        
        for(int i = 0; i < 10; i++) {
            key = i;
            value = _store.get(key);
            assertTrue(value != null);
            
            GenericRecord val = map.get(key);
            assertTrue(val.get("age").equals(value.get("age")));
            assertTrue(val.get("fname").equals(value.get("fname")));
            assertTrue(val.get("lname").equals(value.get("lname")));
        }
    }
}
