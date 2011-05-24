package krati.core.array.entry;

import java.io.IOException;

import krati.io.DataReader;

/**
 * EntryValueIntFactory.
 * 
 * @author jwu
 */
public class EntryValueIntFactory implements EntryValueFactory<EntryValueInt> {
    /**
     * Creates an array of EntryValueInt of a specified length.
     * 
     * @param length
     *            the length of array
     * @return an array of EntryValueInt(s).
     */
    @Override
    public EntryValueInt[] newValueArray(int length) {
        return new EntryValueInt[length];
    }
    
    /**
     * @return an empty EntryValueInt.
     */
    public EntryValueInt newValue() {
        return new EntryValueInt(0, 0, 0);
    }
    
    /**
     * @return an EntryValueInt read from an input stream.
     * @throws IOException
     */
    @Override
    public EntryValueInt newValue(DataReader in) throws IOException {
        return new EntryValueInt(in.readInt(), /* array position */
                                 in.readInt(), /* data value */
                                 in.readLong() /* SCN value */);
    }
    
    /**
     * Read data from stream to populate an EntryValueInt.
     * 
     * @param in
     *            data reader for EntryValueInt.
     * @param value
     *            an EntryValueInt to populate.
     * @throws IOException
     */
    @Override
    public void reinitValue(DataReader in, EntryValueInt value) throws IOException {
        value.reinit(in.readInt(), /* array position */
                     in.readInt(), /* data value */
                     in.readLong() /* SCN value */);
    }
}
