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

package krati.store;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import krati.io.Serializer;
import krati.util.IndexedIterator;

/**
 * ObjectArrayIterator
 * 
 * @author jwu
 * @since 10/08, 2011
 */
public class ObjectArrayIterator<V> implements IndexedIterator<Entry<Integer, V>> {
    private final ArrayStore _store;
    private final Serializer<V> _serializer;
    private int _index;
    
    public ObjectArrayIterator(ArrayStore store, Serializer<V> serializer) {
        this._store = store;
        this._serializer = serializer;
        this._index = store.getIndexStart();
    }
    
    @Override
    public int index() {
        return _index;
    }
    
    @Override
    public void reset(int indexStart) {
        if(_store.hasIndex(indexStart)) {
            _index = indexStart;
            return;
        }
        
        throw new ArrayIndexOutOfBoundsException(indexStart);
    }
    
    @Override
    public boolean hasNext() {
        return _store.hasIndex(_index);
    }
    
    @Override
    public Entry<Integer, V> next() {
        byte[] bytes = _store.get(_index);
        V value = (bytes == null) ? null : _serializer.deserialize(bytes);
        Entry<Integer, V> ret = new SimpleEntry<Integer, V>(_index, value);
        _index++;
        return ret;
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
