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

package krati.store.demo.bus;

import java.util.Random;

import krati.retention.Retention;
import krati.retention.SimpleRetentionStoreWriter;
import krati.retention.RetentionStoreWriter;
import krati.retention.clock.WaterMarksClock;
import krati.store.avro.AvroStore;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

/**
 * StoreBusPersonWriter
 * 
 * @author jwu
 * @since 08/18, 2011
 */
public class StoreBusPersonWriter implements Runnable {
    private Random _rand = new Random();
    private AvroStore<String> _store;
    private RetentionStoreWriter<String, GenericRecord> _writer;
    
    public StoreBusPersonWriter(String source, Retention<String> bus, AvroStore<String> store, WaterMarksClock waterMarksClock) {
        this._store = store;
        this._writer = new SimpleRetentionStoreWriter<String, GenericRecord>(source, bus, store, waterMarksClock);
    }
    
    private GenericRecord createPerson(int memberId) {
        GenericRecord record = new GenericData.Record(_store.getSchema());
        
        record.put("id", memberId);
        record.put("age", _rand.nextInt(100));
        record.put("fname", new Utf8("firstName." + memberId));
        record.put("lname", new Utf8("lastName." + memberId));
        
        return record;
    }
    
    @Override
    public void run() {
        int capacity = _store.capacity();
        int memberId = _rand.nextInt(capacity);
        
        try {
            String key = "member." + memberId;
            GenericRecord value = createPerson(memberId);
            _writer.put(key, value, System.currentTimeMillis());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
