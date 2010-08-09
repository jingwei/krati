package krati.cds.impl.array.entry;

public class PreFillEntryLongDual extends PreFillEntry<EntryValueLongDual>
{
    public PreFillEntryLongDual(int entryId, int capacity)
    {
        super(entryId, new EntryValueLongDualFactory(), capacity);
    }

    @Override
    public void add(EntryValueLongDual value)
    {
        add(value.pos, value.val, value.valDual, value.scn);
    }
    
    /**
     * Adds data to this Entry.
     *  
     * @param pos
     * @param val
     * @param scn
     */
    public void add(int pos, long val, long valDual, long scn)
    {
        if(_index < _entryCapacity)
        {
            _valArray.get(_index++).reinit(pos, val, valDual, scn);
            maintainScn(scn);
        }
        else
        {
            throw new EntryOverflowException(this);
        }
    }
}
