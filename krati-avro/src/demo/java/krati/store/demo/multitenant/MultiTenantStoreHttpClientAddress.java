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

package krati.store.demo.multitenant;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import krati.io.Serializer;
import krati.io.serializer.StringSerializerUtf8;
import krati.store.avro.AvroGenericRecordSerializer;
import krati.store.avro.client.StoreClientHttp;
import krati.store.avro.protocol.StoreDirective;
import krati.util.Chronos;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

/**
 * MultiTenantStoreHttpClientAddress
 * 
 * @author jwu
 * @since 10/01, 2011
 */
public class MultiTenantStoreHttpClientAddress  {
    static Random _rand = new Random();
    
    static Schema createSchema() {
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
    
    static GenericRecord createRecord(Schema schema, int memberId) {
        GenericRecord record = new GenericData.Record(schema);
        
        record.put("id", memberId);
        record.put("city", new Utf8("city." + memberId));
        record.put("state", new Utf8("state." + memberId));
        record.put("country", new Utf8("country." + memberId));
        record.put("postal_code", new Utf8("postal." + memberId));
        
        return record;
    }
    
    public static void main(String[] args) throws Exception {
        String source = "Address";
        Schema schema = createSchema();
        URL url = new URL("http://localhost:8080");
        Serializer<String> keySerializer = new StringSerializerUtf8();
        Serializer<GenericRecord> valueSerializer = new AvroGenericRecordSerializer(schema);
        StoreClientHttp<String, GenericRecord> client = new StoreClientHttp<String, GenericRecord>(
                url, source, keySerializer, valueSerializer);
        
        String ret;
        String key;
        GenericRecord value;
        Chronos c = new Chronos();
        
        // initialize store
        ret = client.send(StoreDirective.StoreInit);
        System.out.println(StoreDirective.StoreInit + ": " + ret);
        System.out.println(c.getElapsedTime());
        
        // schema
        System.out.println(schema.toString());
        
        // put
        for(int i = 0; i < 100; i++) {
            key = "member." + i;
            value = createRecord(schema, i);
            client.put(key, value);
            System.out.println("put: " + key + "->"  + value);
        }
        System.out.println(c.getElapsedTime());
        
        // Sync changes to store
        client.send(StoreDirective.StoreSync);
        System.out.println(StoreDirective.StoreSync + ": " + ret);
        System.out.println(c.getElapsedTime());
        
        // get
        for(int i = 0; i < 100; i++) {
            key = "member." + i;
            value = client.get(key);
            System.out.println("get: " + key + "->"  + value);
        }
        System.out.println(c.getElapsedTime());
        
        // multi-put
        Map<String, GenericRecord> mputMap = new HashMap<String, GenericRecord>();
        for(int i = 100; i < 200; i++) {
            key = "member." + i;
            value = createRecord(schema, i);
            mputMap.put(key, value);
        }
        
        client.put(mputMap);
        System.out.println("mput: " + c.getElapsedTime() + " " + mputMap.size() + " records");
        
        // multi-get
        Map<String, GenericRecord> mgetMap = client.get(mputMap.keySet());
        System.out.println("mget: " + c.getElapsedTime() + " " + mgetMap.size() + " records");
    }
}
