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

package krati.store.demo.bus;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import krati.io.serializer.StringSerializer;
import krati.retention.Position;
import krati.retention.clock.Clock;
import krati.store.bus.client.AvroStoreBusClientHttp;
import krati.util.Chronos;

import org.apache.avro.generic.GenericRecord;

/**
 * AvroStoreBusHttpClient
 * 
 * @author jwu
 * @since 08/18, 2011
 */
public class AvroStoreBusHttpClient {
    
    public static void main(String[] args) throws Exception {
        URL url = new URL("http://localhost:8080");
        String source = "AvroStoreBusHttpServer";
        AvroStoreBusClientHttp<String> client = new AvroStoreBusClientHttp<String>(url, source, new StringSerializer());
        
        System.out.println(client.getSchema());
        
        String key;
        GenericRecord value;
        Chronos c = new Chronos();
        
        for(int i = 0; i < 10; i++) {
            key = "member." + i;
            value = client.get(key);
            System.out.println("get: " + key + "->" + value);
        }
        
        System.out.println(c.getElapsedTime());
        
        System.out.println();
        for(int i = 0; i < 10; i++) {
            System.out.println("position=" + client.getPosition());
            Thread.sleep(100);
        }
        
        System.out.println();
        for(int i = 0; i < 10; i++) {
            System.out.println("position=" + client.getPosition(Clock.ZERO));
            Thread.sleep(100);
        }
        
        Position position, nextPosition;
        Map<String, GenericRecord> map = new HashMap<String, GenericRecord>(1000);
        
        System.out.println();
        c.getElapsedTime();
        
        position = client.getPosition();
        for(int i = 0; i < 10; i++) {
            nextPosition = client.syncUp(position, map);
            System.out.printf("syncUp=%d position=%s in %s%n", map.size(), nextPosition.toString(), c.getElapsedTime());
            position = nextPosition;
            map.clear();
        }
        
        System.out.println();
        c.getElapsedTime();
        
        position = client.getPosition(Clock.ZERO);
        while(true) {
            nextPosition = client.syncUp(position, map);
            System.out.printf("syncUp=%d position=%s in %s%n", map.size(), nextPosition.toString(), c.getElapsedTime());
            if(map.size() == 0) break;
            position = nextPosition;
            map.clear();
        }
        
        System.out.println("syncUp finished");
    }
}
