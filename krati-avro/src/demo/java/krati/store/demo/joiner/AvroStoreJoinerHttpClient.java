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

import java.net.URL;
import java.util.ArrayList;
import java.util.Map;

import krati.io.serializer.StringSerializer;
import krati.store.avro.client.AvroStoreClientHttp;
import krati.util.Chronos;

import org.apache.avro.generic.GenericRecord;

/**
 * AvroStoreJoinerHttpClient
 * 
 * @author jwu
 * @since 08/18, 2011
 */
public class AvroStoreJoinerHttpClient {
    
    public static void main(String[] args) throws Exception {
        URL url = new URL("http://localhost:8080");
        String source = "AvroStoreJoinerHttpServer";
        AvroStoreClientHttp<String> client = new AvroStoreClientHttp<String>(url, source, new StringSerializer());
        
        Chronos c = new Chronos();
        
        // get
        for(int i = 0; i < 100; i++) {
            String key = "member." + i;
            GenericRecord value = client.get(key);
            System.out.println("get: " + key + "->" + value);
        }
        
        System.out.println("total=" + c.getElapsedTime());
        
        // multi-get
        ArrayList<String> keys = new ArrayList<String>();
        for(int i = 0; i < 10; i++) {
            keys.add("member." + i);
        }
        
        Map<String, GenericRecord> map = client.get(keys);
        for(String key : keys) {
            System.out.println("mget: " + key + "->" + map.get(key));
        }
        
        System.out.println("mget: " + c.getElapsedTime());
        
        // multi-put
        client.put(map);
        System.out.println("mput: " + c.getElapsedTime());
        
        // multi-delete
        client.delete(keys);
        System.out.println("mdel: " + c.getElapsedTime());
        
        // multi-get
        Map<String, GenericRecord> map2 = client.get(keys);
        for(String key : keys) {
            System.out.println("mget: " + key + "->" + map2.get(key));
        }
        
        System.out.println("mget: " + c.getElapsedTime());
    }
}
