package krati.core.array.entry;

/**
 * EntryShortFactory.
 * 
 * @author jwu
 *
 */
public class EntryShortFactory implements EntryFactory<EntryValueShort> {
    private int idCounter = 0;
    
    @Override
    public Entry<EntryValueShort> newEntry(int initialCapacity) {
        return new PreFillEntryShort(idCounter++, initialCapacity);
    }
}
