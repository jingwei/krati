package krati.core.array.entry;

/**
 * EntryIntFactory
 * 
 * @author jwu
 */
public class EntryIntFactory implements EntryFactory<EntryValueInt> {
    private int idCounter = 0;
    
    @Override
    public Entry<EntryValueInt> newEntry(int initialCapacity) {
        return new PreFillEntryInt(idCounter++, initialCapacity);
    }
}
