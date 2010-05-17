package krati.cds.impl.array.entry;

/**
 * EntryLongFactory
 *  
 * @author jwu
 */
public class EntryLongFactory implements EntryFactory<EntryValueLong>
{
  private static final EntryValueLongFactory valFactory = new EntryValueLongFactory();

  @Override
  public Entry<EntryValueLong> newEntry(int initialCapacity)
  {
    return new Entry<EntryValueLong>(EntryLongFactory.valFactory, initialCapacity);
  } 
}