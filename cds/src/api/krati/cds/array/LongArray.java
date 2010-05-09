package krati.cds.array;

import krati.cds.MemoryCloneable;

/**
 * Long Array
 * 
 * @author jwu
 *
 */
public interface LongArray extends BasicArray<long[]>, MemoryCloneable
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
