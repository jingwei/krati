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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import test.util.DirUtils;

import junit.framework.TestCase;
import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.segment.SegmentFactory;
import krati.io.Serializer;
import krati.store.avro.AvroStore;
import krati.store.avro.SimpleAvroStore;
import krati.store.avro.AvroStoreJoiner;

/**
 * AbstractTestAvroStoreJoiner
 * 
 * @author jwu
 * @since 09/26, 2011
 */
public abstract class AbstractTestAvroStoreJoiner<K> extends TestCase {
    protected Random _rand = new Random();
    protected AvroStore<K> _personStore;
    protected AvroStore<K> _addressStore;
    protected AvroStoreJoiner<K> _joiner;

    protected abstract K createKey(int memberId);
    
    protected abstract Serializer<K> createKeySerializer();
    
    protected SegmentFactory createSegmentFactory() {
        return new krati.core.segment.WriteBufferSegmentFactory();
    }
    
    protected AvroStore<K> createAvroStore(File storeDir, int capacity, Schema schema, Serializer<K> keySerializer) throws Exception {
        StoreConfig config = new StoreConfig(storeDir, capacity);
        config.setSegmentFactory(createSegmentFactory());
        config.setSegmentFileSizeMB(32);
        config.setNumSyncBatches(2);
        config.setBatchSize(100);
        
        return new SimpleAvroStore<K>(
                StoreFactory.createStaticDataStore(config),
                schema, keySerializer);
    }
    
    /**
     * Creates a joiner store for more than one avro store.
     * 
     * @param homeDir         - store home directory
     * @param initialCapacity - store initial capacity
     * @return A data store for joining multiple individual avro stores.
     * @throws Exception if the joiner cannot be created for any reasons.
     */
    protected AvroStoreJoiner<K> createJoiner(File homeDir, int initialCapacity) throws Exception {
        String source;
        AvroStore<K> store;
        Map<String, AvroStore<K>> map = new HashMap<String, AvroStore<K>>();
        
        // Create "Person" AvroStore
        source = "Person";
        store = createAvroStore(new File(homeDir, source), initialCapacity, createPersonSchema(), createKeySerializer());
        map.put(source, store);
        _personStore = store;
        
        // Create "Address" AvroStore
        source = "Address";
        store = createAvroStore(new File(homeDir, source), initialCapacity, createAddressSchema(), createKeySerializer());
        map.put(source, store);
        _addressStore = store;
        
        // Create joiner
        AvroStoreJoiner<K> joiner = new AvroStoreJoiner<K>("PersonalRecord", "avro.test", map, createKeySerializer());
        joiner.setMaster(_personStore);
        return joiner;
    }
    
    /**
     * Creates the Avro schema for the Person store.
     */
    static Schema createPersonSchema() {
        List<Field> fields = new ArrayList<Field>();
        fields.add(new Field("id", Schema.create(Type.INT), null, null));
        fields.add(new Field("age", Schema.create(Type.INT), null, null));
        fields.add(new Field("fname", Schema.create(Type.STRING), null, null));
        fields.add(new Field("lname", Schema.create(Type.STRING), null, null));
        
        Schema schema = Schema.createRecord("Person", null, "avro.test", false);
        schema.setFields(fields);
        
        return schema;
    }
    
    /**
     * Creates the Avro schema for the Address store.
     */
    static Schema createAddressSchema() {
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
    
    protected GenericRecord createPerson(int memberId) {
        GenericRecord record = new GenericData.Record(_personStore.getSchema());
        
        record.put("id", memberId);
        record.put("age", _rand.nextInt(100));
        record.put("fname", new Utf8("firstName." + memberId));
        record.put("lname", new Utf8("lastName." + memberId));
        
        return record;
    }
    
    private GenericRecord createAddress(int memberId) {
        GenericRecord record = new GenericData.Record(_addressStore.getSchema());
        
        record.put("id", memberId);
        record.put("city", new Utf8("city." + memberId));
        record.put("state", new Utf8("state." + memberId));
        record.put("country", new Utf8("country." + memberId));
        record.put("postal_code", new Utf8("postal." + memberId));
        
        return record;
    }
    
    @Override
    protected void setUp() {
        try {
            File storeDir = DirUtils.getTestDir(getClass());
            _joiner = createJoiner(storeDir, 1000);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            _personStore.close();
            _addressStore.close();
            DirUtils.deleteDirectory(DirUtils.getTestDir(getClass()));
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            _joiner = null;
            _personStore = null;
            _addressStore = null;
        }
    }
    
    public void testApiBasics() throws Exception {
        K key;
        GenericRecord value;
        HashMap<K, GenericRecord> personMap = new HashMap<K, GenericRecord>();
        HashMap<K, GenericRecord> addressMap = new HashMap<K, GenericRecord>();
        
        // put/get
        for(int i = 0; i < 10; i++) {
            key = createKey(i);
            
            value = createPerson(i);
            personMap.put(key, value);
            _personStore.put(key, value);
            
            value = createAddress(i);
            addressMap.put(key, value);
            _addressStore.put(key, value);
        }
        
        _joiner.sync();
        
        for(int i = 0; i < 10; i++) {
            key = createKey(i);
            value = _joiner.get(key);
            assertTrue(value != null);
            
            GenericRecord person = (GenericRecord)value.get("Person");
            GenericRecord address = (GenericRecord)value.get("Address");
            
            GenericRecord p = personMap.get(key);
            assertTrue(p.get("age").equals(person.get("age")));
            assertTrue(p.get("fname").equals(person.get("fname")));
            assertTrue(p.get("lname").equals(person.get("lname")));
            
            GenericRecord a = addressMap.get(key);
            assertTrue(a.get("city").equals(address.get("city")));
            assertTrue(a.get("state").equals(address.get("state")));
            assertTrue(a.get("postal_code").equals(address.get("postal_code")));
        }
        
        // random put/get
        HashSet<Integer> memberIds = new HashSet<Integer>();
        for(int i = 0; i < 10; i++) {
            memberIds.add(_rand.nextInt(_joiner.capacity()));
        }
        
        for(Integer i : memberIds) {
            key = createKey(i);
            
            value = createPerson(i);
            personMap.put(key, value);
            _personStore.put(key, value);
            
            value = createAddress(i);
            addressMap.put(key, value);
            _addressStore.put(key, value);
        }
        
        for(Integer i : memberIds) {
            key = createKey(i);
            
            value = _joiner.get(key);
            GenericRecord person = (GenericRecord)value.get("Person");
            GenericRecord address = (GenericRecord)value.get("Address");
            
            assertTrue(i.equals(person.get("id")));
            assertTrue(i.equals(address.get("id")));
        }
        
        // delete
        for(Integer i : memberIds) {
            key = createKey(i);
            _joiner.delete(key);
        }
        
        for(Integer i : memberIds) {
            key = createKey(i);
            
            value = _joiner.get(key);
            GenericRecord person = (GenericRecord)value.get("Person");
            GenericRecord address = (GenericRecord)value.get("Address");
            
            assertTrue(person == null);
            assertTrue(address == null);
        }
        
        _joiner.sync();
    }
}
