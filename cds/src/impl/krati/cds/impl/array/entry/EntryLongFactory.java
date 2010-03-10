package krati.cds.impl.array.entry;

/**
 * 
 * @author jwu
 */
public class EntryLongFactory implements EntryFactory<EntryValueLong>
{
  private static final EntryValueLongFactory valFactory = new EntryValueLongFactory();
  
  public Entry<EntryValueLong>[] newEntryArray(int length)
  {
    @SuppressWarnings("unchecked")
    Entry<EntryValueLong>[] array = new Entry[length];
    return array;
  }
  
  public Entry<EntryValueLong> newEntry(int initialCapacity)
  {
    return new Entry<EntryValueLong>(EntryLongFactory.valFactory, initialCapacity);
  } 
}