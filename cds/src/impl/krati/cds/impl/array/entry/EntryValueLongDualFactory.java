package krati.cds.impl.array.entry;

import java.io.IOException;

import krati.io.DataReader;

public class EntryValueLongDualFactory implements EntryValueFactory<EntryValueLongDual>
{
    /**
     * Creates an array of EntryValueLongDual of a specified length.
     * 
     * @param length the length of array
     * @return an array of EntryValueLongDual(s).
     */
    public EntryValueLongDual[] newValueArray(int length)
    {
      return new EntryValueLongDual[length];
    }
    
    /**
     * @return an empty EntryValueLongDual.
     */
    public EntryValueLongDual newValue()
    {
      return new EntryValueLongDual(0, 0L, 0L, 0L);
    }
    
    /**
     * @return an EntryValueLongDual read from an input stream.
     * @throws IOException
     */
    public EntryValueLongDual newValue(DataReader in) throws IOException
    {
        return new EntryValueLongDual(in.readInt(),  /* array position */
                                      in.readLong(), /* data value     */
                                      in.readLong(), /* data value dual*/
                                      in.readLong()  /* SCN value      */);
    }
    
    /**
     * Read data from stream to populate an EntryValueLongDual.
     * @param in      data reader for EntryValueLongDual.
     * @param value   an EntryValue to populate.
     * @return <code>true</code> if value is populated.
     * @throws IOException
     */
    @Override
    public void reinitValue(DataReader in, EntryValueLongDual value) throws IOException
    {
        value.reinit(in.readInt(),  /* array position */
                     in.readLong(), /* data value     */
                     in.readLong(), /* data value dual*/
                     in.readLong()  /* SCN value      */);
    }
}
