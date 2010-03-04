package krati.mds.array;

import krati.mds.MemoryCloneable;
import krati.mds.parallel.ParallelDataStore;

/**
 * Long Array
 * 
 * @author jwu
 *
 */
public interface LongArray extends Array, MemoryCloneable, ParallelDataStore<long[]>
{
  /**
   * Gets data at a specified index.
   * 
   * @param index
   * @return data at a specified index
   */
  public long getData(int index);
  
  /**
   * Sets data at a specified index.
   * 
   * @param index
   * @param value
   * @param scn
   */
  public void setData(int index, long value, long scn) throws Exception;
}
