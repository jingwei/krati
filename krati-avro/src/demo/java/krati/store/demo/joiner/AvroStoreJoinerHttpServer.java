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

package krati.store.demo.joiner;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import krati.core.StoreConfig;
import krati.core.segment.MappedSegmentFactory;
import krati.io.Serializer;
import krati.io.serializer.StringSerializer;
import krati.store.DataStore;
import krati.store.avro.AvroStore;
import krati.store.avro.AvroStoreJoiner;
import krati.store.avro.SimpleAvroStore;
import krati.store.avro.protocol.AvroStoreResponder;
import krati.store.avro.protocol.BasicDataStoreResponder;
import krati.store.avro.protocol.BasicDataStoreResponderFactory;
import krati.store.avro.protocol.MultiTenantStoreResponder;
import krati.store.avro.protocol.StoreResponder;
import krati.store.avro.protocol.StoreResponderFactory;
import krati.store.factory.DataStoreFactory;
import krati.store.factory.IndexedDataStoreFactory;
import krati.util.DaemonThreadFactory;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.ipc.HttpServer;

/**
 * AvroStoreJoinerHttpServer
 * 
 * @author jwu
 * @since 08/18, 2011
 */
public class AvroStoreJoinerHttpServer {
    
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
    
    public static void main(String[] args) throws Exception {
        File homeDir = new File(System.getProperty("java.io.tmpdir"), AvroStoreJoinerHttpServer.class.getSimpleName());
        
        // Change initialCapacity accordingly for different data sets
        int initialCapacity = 10000;
        
        // Create store configuration template
        StoreConfig configTemplate = new StoreConfig(homeDir, initialCapacity);
        configTemplate.setSegmentCompactFactor(0.68);
        configTemplate.setSegmentFactory(new MappedSegmentFactory());
        configTemplate.setSegmentFileSizeMB(32);
        configTemplate.setNumSyncBatches(2);
        configTemplate.setBatchSize(100);
        
        // Create multi-tenant store responder
        DataStoreFactory storeFactory = new IndexedDataStoreFactory();
        StoreResponderFactory responderFactory = new BasicDataStoreResponderFactory(storeFactory);
        MultiTenantStoreResponder mtStoreResponder = new MultiTenantStoreResponder(homeDir, configTemplate, responderFactory);
        
        String source;
        StoreResponder responder;
        DataStore<byte[], byte[]> baseStore;
        Serializer<String> keySerializer = new StringSerializer();
        Map<String, AvroStore<String>> map = new HashMap<String, AvroStore<String>>();
        
        // Create "Person" AvroStore
        source = "Person";
        responder = mtStoreResponder.createTenant(source);
        baseStore = ((BasicDataStoreResponder)responder).getStore();
        AvroStore<String> personStore = new SimpleAvroStore<String>(baseStore, createPersonSchema(), keySerializer);
        map.put(source, personStore);
        
        // Create "Address" AvroStore
        source = "Address";
        responder = mtStoreResponder.createTenant(source);
        baseStore = ((BasicDataStoreResponder)responder).getStore();
        AvroStore<String> addressStore = new SimpleAvroStore<String>(baseStore, createAddressSchema(), keySerializer);
        map.put(source, addressStore);
        
        // Create Avro store joiner 
        AvroStoreJoiner<String> joiner = new AvroStoreJoiner<String>("PersonalRecord", "avro.test", map, keySerializer);
        joiner.setMaster(personStore);
        
        // Create writer threads for populating different stores of the joiner
        PersonWriter personWriter = new PersonWriter(personStore);
        AddressWriter addressWriter = new AddressWriter(addressStore);
        
        // Start writer threads with fixed delay
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2, new DaemonThreadFactory());
        executor.scheduleWithFixedDelay(personWriter, 0, 10, TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(addressWriter, 0, 10, TimeUnit.MILLISECONDS);
        
        // Start Avro store joiner server
        HttpServer server = new HttpServer(new AvroStoreResponder<String>(joiner), 8080);
        server.start();
        server.join();
    }
}
