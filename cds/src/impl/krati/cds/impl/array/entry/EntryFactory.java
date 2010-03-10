package krati.cds.impl.array.entry;

/**
 * EntryFactory.
 * 
 * @author jwu
 * 
 * @param <T> Generic type for the basic values contained by an Entry 
 */
public interface EntryFactory<T extends EntryValue>
{
  public Entry<T>[] newEntryArray(int length);
  
  public Entry<T> newEntry(int initialCapacity);
}
