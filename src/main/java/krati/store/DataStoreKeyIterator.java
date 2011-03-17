package krati.store;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import krati.array.DataArray;

/**
 * DataStoreKeyIterator
 * 
 * @author jwu
 * 
 */
final class DataStoreKeyIterator implements Iterator<byte[]> {
    private final ArrayList<byte[]> _keyBucket;
    private final DataStoreHandler _dataHandler;
    private final DataArray _dataArray;
    private int _index = 0;
    
    DataStoreKeyIterator(DataArray dataArray, DataStoreHandler dataHandler) {
        this._dataArray = dataArray;
        this._dataHandler = dataHandler;
        this._keyBucket = new ArrayList<byte[]>(20);
        this.findNext();
    }
    
    @Override
    public boolean hasNext() {
        if(_keyBucket.size() == 0) {
            findNext();
        }
        return _keyBucket.size() > 0;
    }
    
    @Override
    public byte[] next() {
        int size = _keyBucket.size();
        if (size == 0) {
            findNext();
            size = _keyBucket.size();
        }
        
        if(size > 0) {
            return _keyBucket.remove(--size);
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
                    _keyBucket.addAll(keys);
                    break;
                }
            }
        }
    }
}
