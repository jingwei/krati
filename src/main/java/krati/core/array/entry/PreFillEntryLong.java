package krati.core.array.entry;

/**
 * PreFillEntryLong
 * 
 * @author jwu
 * 
 */
public class PreFillEntryLong extends PreFillEntry<EntryValueLong> {
    
    public PreFillEntryLong(int entryId, int capacity) {
        super(entryId, new EntryValueLongFactory(), capacity);
    }
    
    @Override
    public void add(EntryValueLong value) {
        add(value.pos, value.val, value.scn);
    }
    
    /**
     * Adds data to this Entry.
     * 
     * @param pos
     * @param val
     * @param scn
     */
    public void add(int pos, long val, long scn) {
        if (_index < _entryCapacity) {
            _valArray.get(_index++).reinit(pos, val, scn);
            maintainScn(scn);
        } else {
            throw new EntryOverflowException(this);
        }
    }
}
