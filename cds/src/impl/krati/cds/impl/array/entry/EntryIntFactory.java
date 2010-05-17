package krati.cds.impl.array.entry;

/**
 * EntryIntFactory
 * 
 * @author jwu
 */
public class EntryIntFactory implements EntryFactory<EntryValueInt>
{
  private static final EntryValueIntFactory valFactory = new EntryValueIntFactory();
  
  @Override
  public Entry<EntryValueInt> newEntry(int initialCapacity)
  {
    return new Entry<EntryValueInt>(EntryIntFactory.valFactory, initialCapacity);
  } 
}