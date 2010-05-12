package krati.cds.impl.array.basic;

import krati.cds.array.BasicArray;

/**
 * AbstractArray
 * 
 * @author jwu
 *
 * @param <T> Primitive Java Array (e.g., int[], long[]).
 */
public abstract class AbstractArray<T> implements BasicArray<T>
{
  public final int _memberIdCount;
  public final int _memberIdStart; // Inclusive memberId
  public final int _memberIdEnd;   // Exclusive memberId (not in the array)
  protected T      _internalArray;
  
  public AbstractArray(Config config)
  {
    this(config.getMemberIdStart(), config.getMemberIdCount());
  }
  
  public AbstractArray(int memberIdStart, int memberIdCount)
  {
    _memberIdCount = memberIdCount;
    _memberIdStart = memberIdStart;
    _memberIdEnd = _memberIdCount + _memberIdStart;
    init();
  }
  
  protected abstract void init();
  
  @Override
  public T getInternalArray()
  {
    return _internalArray;
  }
  
  public static class Config
  {
    private int _memberIdStart = 0;
    private int _memberIdCount = 0;
    
    /**
     * @return the memberIdStart for the partition
     */
    public int getMemberIdStart()
    {
      return _memberIdStart;
    }
    
    /**
     * @param memberIdStart the memberIdStart to set
     */
    public void setMemberIdStart(int memberIdStart)
    {
      _memberIdStart = memberIdStart;
    }
    
    /**
     * @return the memberIdCount in the partition
     */
    public int getMemberIdCount()
    {
      return _memberIdCount;
    }
    
    /**
     * @param memberIdCount the memberIdCount to set
     */
    public void setMemberIdCount(int memberIdCount)
    {
      _memberIdCount = memberIdCount;
    }
  }
}
