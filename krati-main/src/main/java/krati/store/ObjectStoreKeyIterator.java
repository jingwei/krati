/*
 * Copyright (c) 2010-2011 LinkedIn, Inc
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

import krati.io.Serializer;
import krati.util.IndexedIterator;

/**
 * ObjectStoreKeyIterator
 * 
 * @author  jwu
 * @since   0.3.5
 * @version 0.4.2
 * 
 * <p>
 * 08/10, 2011 - Implemented IndexedIterator <br/>
 */
public class ObjectStoreKeyIterator<K> implements IndexedIterator<K> {
    private final IndexedIterator<byte[]> _rawKeyIterator;
    private final Serializer<K> _keySerializer;
    
    public ObjectStoreKeyIterator(IndexedIterator<byte[]> rawKeyIterator, Serializer<K> keySerializer) {
        this._rawKeyIterator = rawKeyIterator;
        this._keySerializer = keySerializer;
    }
    
    @Override
    public boolean hasNext() {
        return _rawKeyIterator.hasNext();
    }
    
    @Override
    public K next() {
        byte[] rawKey = _rawKeyIterator.next();
        return (rawKey == null) ? null : _keySerializer.deserialize(rawKey);
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public int index() {
        return _rawKeyIterator.index();
    }
    
    @Override
    public void reset(int indexStart) {
        _rawKeyIterator.reset(indexStart);
    }
}
