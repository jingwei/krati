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

import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

import krati.util.IndexedIterator;

/**
 * ArrayStoreIterator
 * 
 * @author jwu
 * @since 10/08, 2011
 */
public class ArrayStoreIterator implements IndexedIterator<Entry<Integer, byte[]>> {
    private final ArrayStore _store;
    private int _index;
    
    public ArrayStoreIterator(ArrayStore store) {
        this._store = store;
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
    public Entry<Integer, byte[]> next() {
        Entry<Integer, byte[]> ret = new SimpleEntry<Integer, byte[]>(_index, _store.get(_index));
        _index++;
        return ret;
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
