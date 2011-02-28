package krati.array;

/**
 * Dynamic Array
 * 
 * @author jwu
 *
 */
public interface DynamicArray extends Array {
  /**
   * Expands the capacity of array to accommodate a given index.
   * 
   * @param index an index in the array
   * @throws Exception
   */
  public void expandCapacity(int index) throws Exception;
}
