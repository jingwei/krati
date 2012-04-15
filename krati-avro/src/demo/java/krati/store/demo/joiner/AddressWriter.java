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

package krati.store.demo.joiner;

import java.util.Random;

import krati.store.avro.AvroStore;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

/**
 * AddressWriter
 * 
 * @author jwu
 * @since 08/18, 2011
 */
public class AddressWriter implements Runnable {
    private Random _rand = new Random();
    private AvroStore<String> _store;
    
    public AddressWriter(AvroStore<String> store) {
        this._store = store;
    }
    
    private GenericRecord createAddress(int memberId) {
        GenericRecord record = new GenericData.Record(_store.getSchema());
        
        record.put("id", memberId);
        record.put("city", new Utf8("city." + memberId));
        record.put("state", new Utf8("state." + memberId));
        record.put("country", new Utf8("country." + memberId));
        record.put("postal_code", new Utf8("postal." + memberId));
        
        return record;
    }
    
    @Override
    public void run() {
        int capacity = _store.capacity();
        int memberId = _rand.nextInt(capacity);
        
        try {
            String key = "member." + memberId;
            GenericRecord value = createAddress(memberId);
            _store.put(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
