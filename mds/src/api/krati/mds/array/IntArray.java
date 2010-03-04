package krati.mds.array;

import krati.mds.MemoryCloneable;
import krati.mds.parallel.ParallelDataStore;

/**
 * Integer Array
 * 
 * @author jwu
 *
 */
public interface IntArray extends Array, MemoryCloneable, ParallelDataStore<int[]>
{
  /**
   * Gets data at a specified index.
   * 
   * @param index
   * @return data at a specified index
   */
  public int getData(int index);
  
  /**
   * Sets data at a specified index.
   * 
   * @param index
   * @param value
   * @param scn
   */
  public void setData(int index, int value, long scn) throws Exception;
}
