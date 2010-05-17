package krati.cds.impl.array.entry;

/**
 * EntryShortFactory.
 * 
 * @author jwu
 *
 */
public class EntryShortFactory implements EntryFactory<EntryValueShort>
{
  private static final EntryValueShortFactory valFactory = new EntryValueShortFactory();

  @Override
  public Entry<EntryValueShort> newEntry(int initialCapacity)
  {
    return new Entry<EntryValueShort>(EntryShortFactory.valFactory, initialCapacity);
  } 
}