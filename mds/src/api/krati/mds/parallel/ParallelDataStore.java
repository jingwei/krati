package krati.mds.parallel;

import krati.mds.Persistable;

public interface ParallelDataStore<T> extends Persistable
{
  public T getParallelData();
  
  public int getMemberIdStart();
  
  public int getMemberIdCount();
  
  public boolean hasMemberId(int memberId);
}
