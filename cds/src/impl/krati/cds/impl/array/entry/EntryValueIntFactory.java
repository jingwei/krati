package krati.cds.impl.array.entry;

import java.io.IOException;

import krati.io.DataReader;

/**
 * EntryValueIntFactory.
 * 
 * @author jwu
 */
public class EntryValueIntFactory implements EntryValueFactory<EntryValueInt>
{
  /**
   * Creates an array of EntryValueInt of a specified length.
   * 
   * @param length the length of array
   * @return an array of EntryValueInt(s).
   */
  public EntryValueInt[] newValueArray(int length)
  {
    assert length >= 0;
    return new EntryValueInt[length];
  }
  
  /**
   * @return an EntryValueInt read from an input stream.
   * @throws IOException
   */
  public EntryValueInt newValue(DataReader in) throws IOException
  {
    return new EntryValueInt(in.readInt(), /* array position */
                             in.readInt(), /* data value     */
                             in.readLong() /* SCN value      */);
  }

}
