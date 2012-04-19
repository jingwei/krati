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

import krati.io.SerializationException;
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
    
    public AvroStoreBusClientHttp(URL serverURL, String source, Serializer<K> keySerializer) {
        super(serverURL, source, keySerializer, null);
    }
    
    @Override
    protected boolean init() {
        boolean ret = super.init();
        _valueSerializer = new LazyAvroGenericRecordSerializer();
        return ret;
    }
    

    /**
     * @return the Avro schema of a remote store.
     */
    public final Schema getSchema() {
        return ((LazyAvroGenericRecordSerializer)_valueSerializer).getSchema();
    }
    
    

    /**
     * A {@link Serializer} implementation that will lazily negotiate an avro {@link Schema} with
     * the remote krati store.
     * 
     * It will keep negotiating until it succeeds once. After that, it will ALWAYS use the same
     * {@link Schema}.
     * All {@link #serialize(GenericRecord)} and {@link #deserialize(byte[])} calls will throw an 
     * {@link IllegalStateException} if a {@link Schema} has not been negotiated yet.
     * 
     * @author dbuthay
     *
     */
    private class LazyAvroGenericRecordSerializer implements Serializer<GenericRecord> {
        private AvroGenericRecordSerializer _delegate = null;
        private Schema _schema = null;

        @Override
        public GenericRecord deserialize(byte[] bytes) throws SerializationException {
            checkSchema();
            return _delegate.deserialize(bytes);

        }
        @Override
        public byte[] serialize(GenericRecord record) throws SerializationException {
            checkSchema();
            return _delegate.serialize(record);
        }

        /**
         * Returns the {@link Schema} negotiated with the remote server or {@code null}
         * if negotiation never succeeded.
         * 
         * Reasons for negotiation not succeeding include
         * <ul>
         *   <li>Network problems</li>
         *   <li>Schema String representation retrieved over the network is not parseable</li>
         * <ul>
         * 
         * NOTE: This method will NOT try to negotiate a Schema.
         * @return the {@link Schema} negotiated with the remote server or {@code null} if negotiation never succeeded.
         */
        public Schema getSchema() {
            return _schema;
        }
        
        
        /**
         * Check if the {@link Schema} has already been negotiated.
         * If not, try to negotiate and fail if not possible
         * 
         * @throws IllegalStateException if there was a problem negotiating the Schema, or
         * if the Schema is invalid.
         */
        private void checkSchema() { 
            if (_delegate == null) {
                // try to create a delegate,
                // First we need to negotiate a Schema
                try { 
                    String prop = getProperty(StoreKeys.KRATI_STORE_VALUE_SCHEMA);
                    _schema = Schema.parse(prop);
                    _delegate = new AvroGenericRecordSerializer(_schema);
                } catch (Exception e) {
                    throw new IllegalStateException("while negotiating Schema: " + e.getMessage(), e);
                }
            }
        }
        
    }
}
