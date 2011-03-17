package krati.core.array.entry;

import java.io.IOException;

import krati.io.DataReader;

/**
 * EntryValueLongFactory.
 * 
 * @author jwu
 */
public class EntryValueLongFactory implements EntryValueFactory<EntryValueLong> {
    /**
     * Creates an array of EntryValueLong of a specified length.
     * 
     * @param length
     *            the length of array
     * @return an array of EntryValueLong(s).
     */
    public EntryValueLong[] newValueArray(int length) {
        return new EntryValueLong[length];
    }
    
    /**
     * @return an empty EntryValueLong.
     */
    public EntryValueLong newValue() {
        return new EntryValueLong(0, 0L, 0L);
    }
    
    /**
     * @return an EntryValueLong read from an input stream.
     * @throws IOException
     */
    public EntryValueLong newValue(DataReader in) throws IOException {
        return new EntryValueLong(in.readInt(),  /* array position */
                                  in.readLong(), /* data value */
                                  in.readLong()  /* SCN value */);
    }
    
    /**
     * Read data from stream to populate an EntryValueLong.
     * 
     * @param in
     *            data reader for EntryValueLong.
     * @param value
     *            an EntryValue to populate.
     * @return <code>true</code> if value is populated.
     * @throws IOException
     */
    @Override
    public void reinitValue(DataReader in, EntryValueLong value) throws IOException {
        value.reinit(in.readInt(),  /* array position */
                     in.readLong(), /* data value */
                     in.readLong()  /* SCN value */);
    }
}
