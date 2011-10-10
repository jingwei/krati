package krati.store;

import krati.util.IndexedIterator;

/**
 * ArrayStoreIndexIterator
 * 
 * @author jwu
 * @since 10/08, 2011
 */
public class ArrayStoreIndexIterator implements IndexedIterator<Integer> {
    private final ArrayStore _store;
    private int _index;
    
    public ArrayStoreIndexIterator(ArrayStore store) {
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
    public Integer next() {
        int ret = _index;
        _index++;
        return ret;
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
