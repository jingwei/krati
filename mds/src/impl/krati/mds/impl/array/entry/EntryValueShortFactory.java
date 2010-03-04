package krati.mds.impl.array.entry;

import java.io.IOException;

import krati.io.ChannelReader;

public class EntryValueShortFactory implements EntryValueFactory<EntryValueShort>
{
  /**
   * Creates an array of EntryValueShort of a specified length.
   * 
   * @param length the length of array
   * @return an array of EntryValueShort(s).
   */
  public EntryValueShort[] newValueArray(int length)
  {
    assert length >= 0;
    return new EntryValueShort[length];
  }
  
  /**
   * @return an EntryValueShort read from an input stream.
   * @throws IOException
   */
  public EntryValueShort newValue(ChannelReader in) throws IOException
  {
    return new EntryValueShort(in.readInt(),   /* array position */
                               in.readShort(), /* data value     */
                               in.readLong()   /* SCN value      */);
  }
}
