package krati.core.array.entry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import krati.io.DataReader;
import krati.io.DataWriter;

/**
 * PreFillEntry
 * 
 * @author jwu
 * 
 */
public abstract class PreFillEntry<T extends EntryValue> extends AbstractEntry<T> {
    protected int _index = 0;
    protected final ArrayList<T> _valArray;
    
    /**
     * Create a new entry to hold updates to an array.
     * 
     * @param entryId
     *            The Id of this Entry.
     * @param valFactory
     *            The factory for manufacturing EntryValue(s).
     * @param initialCapacity
     *            The initial number of values this entry can hold.
     */
    public PreFillEntry(int entryId, EntryValueFactory<T> valFactory, int initialCapacity) {
        super(entryId, valFactory, initialCapacity);
        
        /* PreFill this Entry with empty value(s). */
        _valArray = new ArrayList<T>(initialCapacity);
        for (int i = 0; i < initialCapacity; i++) {
            _valArray.add(_valFactory.newValue());
        }
        
        _index = 0;
    }
    
    @Override
    public final int size() {
        return _index;
    }
    
    @Override
    public void clear() {
        super.clear();
        _index = 0;
    }
    
    @Override
    public List<T> getValueList() {
        return _valArray.subList(0, _index);
    }
    
    @Override
    protected void loadDataSection(DataReader in, int cnt) throws IOException {
        ensureCapacity(cnt);
        
        for (int i = 0; i < cnt; i++) {
            add(_valFactory.newValue(in));
        }
    }
    
    @Override
    protected void saveDataSection(DataWriter out) throws IOException {
        for (T val : getValueList()) {
            val.write(out);
        }
    }
    
    protected void ensureCapacity(int newCapacity) {
        if (newCapacity > _entryCapacity) {
            for (int i = 0, cnt = newCapacity - _entryCapacity; i < cnt; i++) {
                _valArray.add(_valFactory.newValue());
            }
            
            _entryCapacity = newCapacity;
        }
    }
}
