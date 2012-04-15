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

package krati.store.bus.client;

import java.net.URL;

import krati.io.Serializer;
import krati.store.avro.AvroGenericRecordSerializer;
import krati.store.avro.protocol.StoreKeys;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * AvroStoreBusClientHttp
 * 
 * @author jwu
 * @since 10/04, 2011
 */
public class AvroStoreBusClientHttp<K> extends StoreBusClientHttp<K, GenericRecord> {
    protected Schema _schema;
    
    public AvroStoreBusClientHttp(URL serverURL, String source, Serializer<K> keySerializer) {
        super(serverURL, source, keySerializer, null);
    }
    
    @Override
    protected boolean init() {
        boolean ret = super.init();
        
        try {
            String prop = getProperty(StoreKeys.KRATI_STORE_VALUE_SCHEMA);
            _schema = Schema.parse(prop);
            _valueSerializer = new AvroGenericRecordSerializer(_schema);
        } catch(Exception e) {
            ret = false;
        }
        
        return ret;
    }
    
    /**
     * @return the Avro schema of a remote store.
     */
    public final Schema getSchema() {
        if(_schema == null) {
            init();
        }
        
        return _schema;
    }
}
