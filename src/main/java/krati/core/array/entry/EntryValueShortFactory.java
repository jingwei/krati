package krati.core.array.entry;

import java.io.IOException;

import krati.io.DataReader;

/**
 * EntryValueShortFactory
 * 
 * @author jwu
 * 
 */
public class EntryValueShortFactory implements EntryValueFactory<EntryValueShort> {
    /**
     * Creates an array of EntryValueShort of a specified length.
     * 
     * @param length
     *            the length of array
     * @return an array of EntryValueShort(s).
     */
    public EntryValueShort[] newValueArray(int length) {
        return new EntryValueShort[length];
    }
    
    /**
     * @return an empty EntryValueShort.
     */
    public EntryValueShort newValue() {
        return new EntryValueShort(0, (short) 0, 0L);
    }
    
    /**
     * @return an EntryValueShort read from an input stream.
     * @throws IOException
     */
    public EntryValueShort newValue(DataReader in) throws IOException {
        return new EntryValueShort(in.readInt(),   /* array position */
                                   in.readShort(), /* data value */
                                   in.readLong()   /* SCN value */);
    }
    
    /**
     * Read data from stream to populate an EntryValueShort.
     * 
     * @param in
     *            data reader for EntryValueShort.
     * @param value
     *            an EntryValue to populate.
     * @throws IOException
     */
    @Override
    public void reinitValue(DataReader in, EntryValueShort value) throws IOException {
        value.reinit(in.readInt(),   /* array position */
                     in.readShort(), /* data value */
                     in.readLong()   /* SCN value */);
    }
}
