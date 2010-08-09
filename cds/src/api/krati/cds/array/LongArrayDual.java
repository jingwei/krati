package krati.cds.array;

public interface LongArrayDual extends LongArray
{
    /**
     * Gets data at a specified index.
     * 
     * @param index
     * @return data at a specified index
     */
    public long getDual(int index);
    
    /**
     * Sets data at a specified index.
     * 
     * @param index
     * @param value
     * @param scn
     */
    public void setDual(int index, long value, long scn) throws Exception;
    
    /**
     * Gets the internal primitive array.
     * 
     * @return long array.
     */
    public long[] getInternalArrayDual();
    
    /**
     * Sets data at a specified index.
     * 
     * @param index
     * @param value
     * @param valueDual
     * @param scn
     */
    public void set(int index, long value, long valueDual, long scn) throws Exception;
    
}
