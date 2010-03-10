package krati.cds.array;

import krati.cds.Persistable;

/**
 * Array
 * 
 * @author jwu
 *
 */
public interface Array extends Persistable
{
  /**
   * Clears this Array.
   */
  public void clear();
  
  /**
   * @return the current length of this Array.
   */
  public int length();
  
  /**
   * @return the start index of this Array.
   */
  public int getIndexStart();
  
  /**
   * @return a boolean indicating an index is in the range of this Array.
   */
  public boolean indexInRange(int index);
}
