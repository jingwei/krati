package krati.core.array.entry;

/**
 * EntryFactory.
 * 
 * @author jwu
 * 
 * @param <T> Generic type for the basic values contained by an Entry
 */
public interface EntryFactory<T extends EntryValue> {
    
    public Entry<T> newEntry(int initialCapacity);
    
}
