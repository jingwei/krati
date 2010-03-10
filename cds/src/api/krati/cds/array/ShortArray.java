package krati.cds.array;

import krati.cds.parallel.ParallelDataStore;

/**
 * Short Array
 * 
 * @author jwu
 *
 */
public interface ShortArray extends Array, ParallelDataStore<short[]>
{
  /**
   * Gets data at a specified index.
   * 
   * @param index
   * @return data at a specified index
   */
  public short getData(int index);
  
  /**
   * Sets data at a specified index.
   * 
   * @param index
   * @param value
   * @param scn
   */
  public void setData(int index, short value, long scn) throws Exception;
}
