package krati.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import krati.array.DataArray;

/**
 * DataStoreIterator
 * 
 * @author jwu
 * 
 */
final class DataStoreIterator implements Iterator<Entry<byte[], byte[]>> {
    private final ArrayList<Entry<byte[], byte[]>> _keyBucket;
    private final DataStoreHandler _dataHandler;
    private final DataArray _dataArray;
    private int _index = 0;
    
    DataStoreIterator(DataArray dataArray, DataStoreHandler dataHandler) {
        this._dataArray = dataArray;
        this._dataHandler = dataHandler;
        this._keyBucket = new ArrayList<Entry<byte[], byte[]>>(20);
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
    public Entry<byte[], byte[]> next() {
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
                List<Entry<byte[], byte[]>> entries = _dataHandler.extractEntries(data);
                if(entries != null && entries.size() > 0) {
                    _keyBucket.addAll(entries);
                    break;
                }
            }
        }
    }
}
