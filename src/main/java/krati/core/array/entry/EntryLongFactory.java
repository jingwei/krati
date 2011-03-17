package krati.core.array.entry;

/**
 * EntryLongFactory
 * 
 * @author jwu
 */
public class EntryLongFactory implements EntryFactory<EntryValueLong> {
    private int idCounter = 0;
    
    @Override
    public Entry<EntryValueLong> newEntry(int initialCapacity) {
        return new PreFillEntryLong(idCounter++, initialCapacity);
    }
}
