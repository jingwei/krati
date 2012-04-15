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

package krati.store.avro;

import krati.io.Serializer;
import krati.store.SerializableObjectStore;
import krati.store.DataStore;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * AvroStoreImpl
 * 
 * @author jwu
 * @since 08/07, 2011
 */
public class SimpleAvroStore<K> extends SerializableObjectStore<K, GenericRecord> implements AvroStore<K> {
    private final Schema _schema;
    
    /**
     * Creates a AvroStore.
     * 
     * @param store         - the data store
     * @param schema        - the Avro schema
     * @param keySerializer - the key serialize
     */
    public SimpleAvroStore(DataStore<byte[], byte[]> store, Schema schema, Serializer<K> keySerializer) {
        super(store, keySerializer, new AvroGenericRecordSerializer(schema));
        this._schema = schema;
    }
    
    /**
     * @return the Avro schema of this store.
     */
    @Override
    public final Schema getSchema() {
        return _schema;
    }
}
