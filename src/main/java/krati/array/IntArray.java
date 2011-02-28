package krati.array;

/**
 * Integer Array
 * 
 * @author jwu
 *
 */
public interface IntArray extends Array {
  /**
   * Gets data at a specified index.
   * 
   * @param index
   * @return data at a specified index
   */
  public int get(int index);
  
  /**
   * Sets data at a specified index.
   * 
   * @param index
   * @param value
   * @param scn
   */
  public void set(int index, int value, long scn) throws Exception;
  
  /**
   * Gets the internal primitive array.
   * 
   * @return int array.
   */
  public int[] getInternalArray();
}
