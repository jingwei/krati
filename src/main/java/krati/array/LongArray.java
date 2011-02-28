package krati.array;

/**
 * Long Array
 * 
 * @author jwu
 *
 */
public interface LongArray extends Array {
  /**
   * Gets data at a specified index.
   * 
   * @param index
   * @return data at a specified index
   */
  public long get(int index);
  
  /**
   * Sets data at a specified index.
   * 
   * @param index
   * @param value
   * @param scn
   */
  public void set(int index, long value, long scn) throws Exception;
  
  /**
   * Gets the internal primitive array.
   * 
   * @return long array.
   */
  public long[] getInternalArray();
}
