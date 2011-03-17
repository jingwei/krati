package krati.core.array.entry;

/**
 * PreFillEntryShort
 * 
 * @author jwu
 * 
 */
public class PreFillEntryShort extends PreFillEntry<EntryValueShort> {
    
    public PreFillEntryShort(int entryId, int capacity) {
        super(entryId, new EntryValueShortFactory(), capacity);
    }
    
    @Override
    public void add(EntryValueShort value) {
        add(value.pos, value.val, value.scn);
    }
    
    /**
     * Adds data to this Entry.
     * 
     * @param pos
     * @param val
     * @param scn
     */
    public void add(int pos, short val, long scn) {
        if (_index < _entryCapacity) {
            _valArray.get(_index++).reinit(pos, val, scn);
            maintainScn(scn);
        } else {
            throw new EntryOverflowException(this);
        }
    }
}
