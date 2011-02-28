package krati.array;

import java.nio.channels.WritableByteChannel;

/**
 * Data Array
 * 
 * @author jwu
 *
 */
public interface DataArray extends Array {
    /**
     * @return <code>true</code> if data exists at a given index. Otherwise, <code>false</code>.
     */
    public boolean hasData(int index);
    
    /**
     * @return the length of data at a specified index
     */
    public int getLength(int index);
    
    /**
     * Gets data at a specified index.
     * 
     * @param index  data index in DataArray
     * @return       data at a specified index
     */
    public byte[] get(int index);
    
    /**
     * Gets data at a specified index into a byte array.
     * 
     * @param index  data index in DataArray
     * @param dst    the byte array to write to 
     * @return       the number of bytes written to the byte array.
     */
    public int get(int index, byte[] dst);
    
    /**
     * Gets data at a specified index into a byte array.
     * 
     * @param index  data index in DataArray
     * @param dst    the byte array to write to 
     * @param offset the offset of byte array from where data will be written
     * @return       the number of bytes written to the byte array.
     */
    public int get(int index, byte[] dst, int offset);
    
    /**
     * Sets data at a specified index.
     * 
     * @param index  data index in DataArray
     * @param data   data to write to DataArray
     * @param scn
     */
    public void set(int index, byte[] data, long scn) throws Exception;

    /**
     * Sets data at a specified index.
     * 
     * @param index  data index in DataArray
     * @param data   data to write to DataArray
     * @param offset the offset of the data array to start read
     * @param length the length of data to read from the data array
     * @param scn
     */
    public void set(int index, byte[] data, int offset, int length, long scn) throws Exception;
    
    /**
     * Transfers data at a given index to a writable file channel.
     * 
     * @param index   data index in DataArray
     * @param channel channel to transfer data to
     * @return        the number of bytes transferred.
     */
    public int transferTo(int index, WritableByteChannel channel);
    
}
