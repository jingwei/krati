package krati.mds.impl.array.entry;

import java.io.IOException;

import krati.io.DataReader;

/**
 * EntryValueLongFactory.
 * 
 * @author jwu
 */
public class EntryValueLongFactory implements EntryValueFactory<EntryValueLong>
{
  /**
   * Creates an array of EntryValueLong of a specified length.
   * 
   * @param length the length of array
   * @return an array of EntryValueLong(s).
   */
  public EntryValueLong[] newValueArray(int length)
  {
    assert length >= 0;
    return new EntryValueLong[length];
  }
  
  /**
   * @return an EntryValueLong read from an input stream.
   * @throws IOException
   */
  public EntryValueLong newValue(DataReader in) throws IOException
  {
    return new EntryValueLong(in.readInt(),  /* array position */
                              in.readLong(), /* data value     */
                              in.readLong()  /* SCN value      */);
  }

}
