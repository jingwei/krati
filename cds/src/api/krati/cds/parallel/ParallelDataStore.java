package krati.cds.parallel;

import krati.cds.Persistable;

public interface ParallelDataStore<T> extends Persistable
{
  public T getParallelData();
  
  public int getMemberIdStart();
  
  public int getMemberIdCount();
  
  public boolean hasMemberId(int memberId);
}
