package krati.core.array.entry;

/**
 * PreFillEntryInt
 * 
 * @author jwu
 * 
 */
public class PreFillEntryInt extends PreFillEntry<EntryValueInt> {
    
    public PreFillEntryInt(int entryId, int capacity) {
        super(entryId, new EntryValueIntFactory(), capacity);
    }
    
    @Override
    public void add(EntryValueInt value) {
        add(value.pos, value.val, value.scn);
    }
    
    /**
     * Adds data to this Entry.
     * 
     * @param pos
     * @param val
     * @param scn
     */
    public void add(int pos, int val, long scn) {
        if (_index < _entryCapacity) {
            _valArray.get(_index++).reinit(pos, val, scn);
            maintainScn(scn);
        } else {
            throw new EntryOverflowException(this);
        }
    }
}
