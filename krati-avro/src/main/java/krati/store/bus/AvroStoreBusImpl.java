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

package krati.store.bus;

import java.util.List;
import java.util.Map;

import krati.retention.Event;
import krati.retention.Position;
import krati.retention.Retention;
import krati.retention.RetentionStoreReader;
import krati.retention.SimpleRetentionStoreReader;
import krati.retention.clock.Clock;
import krati.store.avro.AvroStore;

import org.apache.avro.generic.GenericRecord;

/**
 * AvroStoreBusImpl
 * 
 * @author jwu
 * @since 09/21, 2011
 */
public class AvroStoreBusImpl<K> implements AvroStoreBus<K> {
    private final AvroStore<K> _store;
    private final Retention<K> _retention;
    private final RetentionStoreReader<K, GenericRecord> _reader; 
    
    /**
     * Creates a AvroStoreBus.
     * 
     * @param source    - the source
     * @param retention - the retention
     * @param store     - the data store
     */
    public AvroStoreBusImpl(String source, Retention<K> retention, AvroStore<K> store) {
        this._store = store;
        this._retention = retention;
        this._reader = new SimpleRetentionStoreReader<K, GenericRecord>(source, retention, store);
    }
    
    @Override
    public AvroStore<K> getStore() {
        return _store;
    }
    
    @Override
    public Retention<K> getRetention() {
        return _retention;
    }
    
    @Override
    public String getSource() {
        return _reader.getSource();
    }
    
    @Override
    public Position getPosition() {
        return _reader.getPosition();
    }
    
    @Override
    public Position getPosition(Clock c) {
        return _reader.getPosition(c);
    }
    
    @Override
    public GenericRecord get(K key) throws Exception {
        return key == null ? null : _store.get(key);
    }
    
    @Override
    public Position get(Position pos, Map<K, Event<GenericRecord>> map) {
        return _reader.get(pos, map);
    }
    
    @Override
    public Position get(Position pos, List<Event<K>> list) {
        return _reader.get(pos, list);
    }
}
