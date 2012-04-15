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

import java.util.List;
import java.util.ArrayList;
import java.util.NoSuchElementException;

import krati.array.DataArray;
import krati.util.IndexedIterator;

/**
 * DataStoreKeyIterator
 * 
 * @author  jwu
 * @since   0.3.5
 * @version 0.4.2
 * 
 * <p>
 * 08/10, 2011 - Implemented IndexedIterator <br/>
 */
final class DataStoreKeyIterator implements IndexedIterator<byte[]> {
    private final ArrayList<byte[]> _bucket;
    private final DataStoreHandler _dataHandler;
    private final DataArray _dataArray;
    private int _index = 0;
    
    DataStoreKeyIterator(DataArray dataArray, DataStoreHandler dataHandler) {
        this._dataArray = dataArray;
        this._dataHandler = dataHandler;
        this._bucket = new ArrayList<byte[]>(20);
        this.findNext();
    }
    
    @Override
    public boolean hasNext() {
        if(_bucket.size() == 0) {
            findNext();
        }
        return _bucket.size() > 0;
    }
    
    @Override
    public byte[] next() {
        int size = _bucket.size();
        if (size == 0) {
            findNext();
            size = _bucket.size();
        }
        
        if(size > 0) {
            return _bucket.remove(--size);
        }
        
        throw new NoSuchElementException();
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    private void findNext() {
        while(_index < _dataArray.length()) {
            byte[] data = _dataArray.get(_index++);
            
            if(data != null) {
                List<byte[]> keys = _dataHandler.extractKeys(data);
                if(keys != null && keys.size() > 0) {
                    _bucket.addAll(keys);
                    break;
                }
            }
        }
    }
    
    @Override
    public int index() {
        return _index;
    }
    
    @Override
    public void reset(int indexStart) {
        _index = Math.max(0, indexStart);
        _bucket.clear();
        findNext();
    }
}
