package krati.core.array.entry;

import java.io.IOException;

import krati.io.DataReader;

/**
 * EntryValueFactory.
 * 
 * @author jwu
 * 
 * @param <T> Generic type of EntryValue
 */
public interface EntryValueFactory<T extends EntryValue> {
    /**
     * Creates an array of EntryValue of a specified length.
     * 
     * @param length
     *            the length of array.
     * @return an array of EntryValue(s).
     */
    public T[] newValueArray(int length);

    /**
     * @return an empty EntryValue.
     */
    public T newValue();

    /**
     * @return an EntryValue read from an input stream.
     * @throws IOException
     */
    public T newValue(DataReader in) throws IOException;

    /**
     * Read data from stream to populate an EntryValue.
     * 
     * @param in
     *            data reader for EntryValue.
     * @param value
     *            an EntryValue to populate.
     * @return <code>true</code> if value is populated.
     * @throws IOException
     */
    public void reinitValue(DataReader in, T value) throws IOException;
}
