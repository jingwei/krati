package krati.array;

/**
 * Short Array
 * 
 * @author jwu
 *
 */
public interface ShortArray extends Array {
  /**
   * Gets data at a specified index.
   * 
   * @param index
   * @return data at a specified index
   */
  public short get(int index);
  
  /**
   * Sets data at a specified index.
   * 
   * @param index
   * @param value
   * @param scn
   */
  public void set(int index, short value, long scn) throws Exception;
  
  /**
   * Gets the internal primitive array.
   * 
   * @return short array.
   */
  public short[] getInternalArray();
}
