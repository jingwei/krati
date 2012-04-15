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

package krati.store.demo.http;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.segment.MemorySegmentFactory;
import krati.store.DataStore;
import krati.store.avro.protocol.BasicDataStoreResponder;
import krati.store.avro.protocol.StoreKeys;
import krati.store.avro.protocol.StoreResponder;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.ipc.HttpServer;

/**
 * SingleAvroStoreHttpServer
 * 
 * @author jwu
 * @since 08/07, 2011
 */
public class SingleAvroStoreHttpServer {
    
    static DataStore<byte[], byte[]> createDataStore(File storeDir, int capacity) throws Exception {
        StoreConfig config = new StoreConfig(storeDir, capacity);
        config.setSegmentFactory(new MemorySegmentFactory());
        config.setSegmentFileSizeMB(32);
        config.setNumSyncBatches(2);
        config.setBatchSize(100);
        
        return StoreFactory.createStaticDataStore(config);
    }
    
    static Schema createSchema() {
        List<Field> fields = new ArrayList<Field>();
        fields.add(new Field("id", Schema.create(Type.INT), null, null));
        fields.add(new Field("age", Schema.create(Type.INT), null, null));
        fields.add(new Field("fname", Schema.create(Type.STRING), null, null));
        fields.add(new Field("lname", Schema.create(Type.STRING), null, null));
        
        Schema schema = Schema.createRecord("Person", null, "avro.test", false);
        schema.setFields(fields);
        
        return schema;
    }
    
    public static void main(String[] args) throws Exception {
        File storeDir = new File(System.getProperty("java.io.tmpdir"), SingleAvroStoreHttpServer.class.getSimpleName());
        StoreResponder storeResponder = new BasicDataStoreResponder(createDataStore(storeDir, 10000));
        storeResponder.setProperty(StoreKeys.KRATI_STORE_VALUE_SCHEMA, createSchema().toString());
        HttpServer server = new HttpServer(storeResponder, 8080);
        server.start();
        server.join();
    }
}
