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

package krati.store.avro;

import java.nio.ByteOrder;

import krati.io.Serializer;
import krati.io.serializer.IntSerializer;
import krati.store.ArrayStore;
import krati.store.SerializableObjectArray;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * SimpleAvroArray
 * 
 * @author jwu
 * @since 09/25, 2011
 */
public class SimpleAvroArray extends SerializableObjectArray<GenericRecord> implements AvroStore<Integer> {
    private final Schema _schema;
    
    /**
     * Creates an array-like AvroStore.
     * 
     * @param store  - the array store
     * @param schema - the Avro schema
     */
    public SimpleAvroArray(ArrayStore store, Schema schema) {
        super(store, new IntSerializer(ByteOrder.BIG_ENDIAN), new AvroGenericRecordSerializer(schema));
        this._schema = schema;
    }
    
    /**
     * Creates an array-like AvroStore.
     * 
     * @param store  - the array store
     * @param schema - the Avro schema
     * @param keySerializer - the key (integer) serializer
     */
    public SimpleAvroArray(ArrayStore store, Schema schema, Serializer<Integer> keySerializer) {
        super(store, keySerializer, new AvroGenericRecordSerializer(schema));
        this._schema = schema;
    }
    
    /**
     * @return the Avro schema of this store.
     */
    @Override
    public Schema getSchema() {
        return _schema;
    }
}
