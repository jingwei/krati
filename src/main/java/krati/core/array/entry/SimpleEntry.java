package krati.core.array.entry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import krati.io.DataReader;
import krati.io.DataWriter;

/**
 * SimpleEntry
 * 
 * Transactional Redo Entry.
 * 
 * @author jwu
 */
public class SimpleEntry<T extends EntryValue> extends AbstractEntry<T> {
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
    public SimpleEntry(int entryId, EntryValueFactory<T> valFactory, int initialCapacity) {
        super(entryId, valFactory, initialCapacity);
        this._valArray = new ArrayList<T>(initialCapacity);
    }
    
    @Override
    public int size() {
        return _valArray.size();
    }
    
    @Override
    public void clear() {
        super.clear();
        _valArray.clear();
    }
    
    @Override
    public void add(T value) {
        _valArray.add(value);
        maintainScn(value.scn);
    }
    
    @Override
    public List<T> getValueList() {
        return _valArray;
    }
    
    @Override
    protected void loadDataSection(DataReader in, int cnt) throws IOException {
        for (int i = 0; i < cnt; i++) {
            _valArray.add(_valFactory.newValue(in));
        }
    }
    
    @Override
    protected void saveDataSection(DataWriter out) throws IOException {
        for (T val : _valArray) {
            val.write(out);
        }
    }
}
