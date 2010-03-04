package krati.mds.impl.array.entry;

/**
 * 
 * @author jwu
 */
public class EntryIntFactory implements EntryFactory<EntryValueInt>
{
  private static final EntryValueIntFactory valFactory = new EntryValueIntFactory();
  
  public Entry<EntryValueInt>[] newEntryArray(int length)
  {
    @SuppressWarnings("unchecked")
    Entry<EntryValueInt>[] array = new Entry[length];
    return array;
  }
  
  public Entry<EntryValueInt> newEntry(int initialCapacity)
  {
    return new Entry<EntryValueInt>(EntryIntFactory.valFactory, initialCapacity);
  } 
}