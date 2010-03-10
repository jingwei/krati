package krati.cds.parallel;

import krati.cds.parallel.ParallelDataStore;

public abstract class AbstractParallelDataStore<T> implements ParallelDataStore<T>
{
  public final int _memberIdCount;
  public final int _memberIdStart; // Inclusive memberId in parallel data store
  public final int _memberIdEnd;   // Exclusive memberId not in parallel data store
  protected T      _parallelData;
  
  public AbstractParallelDataStore(Config config)
  {
    this(config.getMemberIdStart(), config.getMemberIdCount());
  }
  
  public AbstractParallelDataStore(int memberIdStart, int memberIdCount)
  {
    _memberIdCount = memberIdCount;
    _memberIdStart = memberIdStart;
    _memberIdEnd = _memberIdCount + _memberIdStart;
    init();
  }
  
  protected void init()
  {
    // Subclass of AbstractParallellDataStore should override this method
  }
  
  public boolean hasMemberId(int memberId)
  {
    return (_memberIdStart <= memberId && memberId < _memberIdEnd) ;
  }
  
  public int getMemberIdStart()
  {
    return _memberIdStart;
  }
  
  public int getMemberIdCount()
  {
    return _memberIdCount;
  }
  
  public T getParallelData()
  {
    return _parallelData;
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
