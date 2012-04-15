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

import krati.io.Serializer;
import krati.store.ObjectStore;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * AvroStore
 * 
 * @author jwu
 * @since 08/06, 2011
 */
public interface AvroStore<K> extends ObjectStore<K, GenericRecord> {

    /**
     * @return the store schema.
     */
    public Schema getSchema();
    
    /**
     * @return the key serializer.
     */
    public Serializer<K> getKeySerializer();
    
    /**
     * @return the Avro record serializer
     */
    public Serializer<GenericRecord> getValueSerializer();
    
    /**
     * Gets an Avro record in the form of byte array from this store.
     * 
     * @param key - the retrieving key. 
     * @return the retrieved record in raw bytes.
     */
    @Override
    public byte[] getBytes(K key);
    
    /**
     * Gets an Avro record in the form of byte array from the store.
     * 
     * @param keyBytes - the retrieving key in raw bytes. 
     * @return the retrieved record in raw bytes.
     */
    @Override
    public byte[] getBytes(byte[] keyBytes);
}
