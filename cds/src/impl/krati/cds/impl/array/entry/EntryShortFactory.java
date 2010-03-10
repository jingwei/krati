package krati.cds.impl.array.entry;

public class EntryShortFactory implements EntryFactory<EntryValueShort>
{
  private static final EntryValueShortFactory valFactory = new EntryValueShortFactory();
  
  public Entry<EntryValueShort>[] newEntryArray(int length)
  {
    @SuppressWarnings("unchecked")
    Entry<EntryValueShort>[] array = new Entry[length];
    return array;
  }
  
  public Entry<EntryValueShort> newEntry(int initialCapacity)
  {
    return new Entry<EntryValueShort>(EntryShortFactory.valFactory, initialCapacity);
  } 
}