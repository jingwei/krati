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

package test.store.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import krati.io.serializer.StringSerializerUtf8;
import krati.store.avro.client.StoreClient;
import krati.store.avro.client.StoreClientLocal;
import krati.store.avro.protocol.ProtocolConstants;
import krati.store.avro.protocol.StoreDirective;

/**
 * TestStoreClientLocal
 * 
 * @author jwu
 * @since 10/03, 2011
 */
public class TestStoreClientLocal extends AbstractTestStoreClient<String, String> {
    protected final Random _rand = new Random();
    
    @Override
    protected StoreClient<String, String> createStoreClient() {
        return new StoreClientLocal<String, String>(
                createStoreResponder(),
                getClass().getSimpleName(),
                new StringSerializerUtf8(),
                new StringSerializerUtf8());
    }
    
    public void testApiBasics() throws Exception {
        String key, value;
        Map<String, String> map = new HashMap<String, String>();
        StoreClient<String, String> client = createStoreClient();
        
        // get
        key = "member.100";
        value = client.get(key);
        assertTrue(value == null);
        assertFalse(client.delete(key));
        assertFalse(client.put(key, null));
        
        // put/get/delete
        for(int i = 0; i < 100; i++) {
            key = "member." + _rand.nextInt(Integer.MAX_VALUE);
            value = "Here is the profile for " + key;
            
            map.put(key, value);
            assertTrue(client.put(key, value));
            assertEquals(value, client.get(key));
            
            assertTrue(client.delete(key));
            assertEquals(null, client.get(key));
        }
        
        // store directive
        assertEquals(ProtocolConstants.NOP, client.send(StoreDirective.StoreInit));
        assertEquals(ProtocolConstants.SUC, client.send(StoreDirective.StoreSync));
        
        // multi-get
        Map<String, String> resultMap = client.get(map.keySet());
        assertEquals(0, resultMap.size());
        
        // multi-put
        assert(client.put(map));
        
        // multi-get
        resultMap = client.get(map.keySet());
        assertEquals(map.size(), resultMap.size());
        
        for(Map.Entry<String, String> e : map.entrySet()) {
            assertEquals(e.getValue(), resultMap.get(e.getKey()));
        }
        
        for(Map.Entry<String, String> e : map.entrySet()) {
            assertEquals(e.getValue(), client.get(e.getKey()));
        }
        
        // multi-delete
        assertTrue(client.delete(map.keySet()));
        resultMap = client.get(map.keySet());
        assertEquals(0, resultMap.size());
    }
    
    public void testProperties() throws Exception {
        String key, value;
        StoreClient<String, String> client = createStoreClient();
        
        key = "KeySerializer";
        value = StringSerializerUtf8.class.getCanonicalName();
        assertEquals(null, client.getProperty(key));
        assertEquals(null, client.setProperty(key, value));
        assertEquals(value, client.setProperty(key, value));
        assertEquals(value, client.setProperty(key, null));
        assertEquals(null, client.getProperty(key));
        assertEquals(null, client.setProperty(key, null));
    }
    
    public void testNullCases() throws Exception {
        Map<String, String> resultMap;
        StoreClient<String, String> client = createStoreClient();
        
        // multi-put with empty map
        assertFalse(client.put(new HashMap<String, String>()));
        
        // create a map with null values
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("member.1", "member.1.data");
        map1.put("member.2", "member.2.data");
        map1.put("member.3", null);
        
        // create a map with non-null values
        Map<String, String> map2 = new HashMap<String, String>();
        map2.put("member.1", "member.1.data");
        map2.put("member.2", "member.2.data");
        map2.put("member.3", "member.3.data");
        
        // multi-put with null values
        assertTrue(client.put(map1));
        
        resultMap = client.get(map1.keySet());
        
        assertEquals(2, resultMap.size());
        assertTrue(resultMap.containsKey("member.1"));
        assertTrue(resultMap.containsKey("member.2"));
        assertFalse(resultMap.containsKey("member.3"));
        
        // multi-put with non-null values
        assertTrue(client.put(map2));
        
        resultMap = client.get(map2.keySet());
        
        assertEquals(3, resultMap.size());
        assertTrue(resultMap.containsKey("member.1"));
        assertTrue(resultMap.containsKey("member.2"));
        assertTrue(resultMap.containsKey("member.3"));
        
        // multi-put with null values
        assertTrue(client.put(map1));
        
        resultMap = client.get(map1.keySet());
        
        assertEquals(2, resultMap.size());
        assertTrue(resultMap.containsKey("member.1"));
        assertTrue(resultMap.containsKey("member.2"));
        assertFalse(resultMap.containsKey("member.3"));
        
        // multi-get with null values
        List<String> keys = new ArrayList<String>();
        keys.add("member.1");
        keys.add(null);
        keys.add("member.2");
        
        resultMap = client.get(keys);
        
        assertEquals(2, resultMap.size());
        assertTrue(resultMap.containsKey("member.1"));
        assertTrue(resultMap.containsKey("member.2"));
        
        // multi-delete with null values
        assertTrue(client.delete(keys));

        resultMap = client.get(keys);
        
        assertEquals(0, resultMap.size());
        
        // multi-get and multi-delete for empty list
        keys = new ArrayList<String>();;
        resultMap = client.get(keys);
        assertEquals(0, resultMap.size());
        assertFalse(client.delete(keys));
    }
}
