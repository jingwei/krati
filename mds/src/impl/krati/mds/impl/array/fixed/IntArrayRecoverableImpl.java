package krati.mds.impl.array.fixed;

import java.io.File;

import org.apache.log4j.Logger;

import krati.mds.array.IntArray;
import krati.mds.impl.array.entry.EntryIntFactory;
import krati.mds.impl.array.entry.EntryValueInt;

public class IntArrayRecoverableImpl extends RecoverableArrayImpl<int[], EntryValueInt> implements IntArray
{
  private static final Logger log = Logger.getLogger(IntArrayRecoverableImpl.class);
  
  public IntArrayRecoverableImpl(Config config) throws Exception
  {
    this(config.getMemberIdStart(),
         config.getMemberIdCount(),
         config.getMaxEntrySize(),
         config.getMaxEntries(),
         config.getCacheDirectory());
  }
  
  public IntArrayRecoverableImpl(int memberIdStart,
                                 int memberIdCount,
                                 int maxEntrySize,
                                 int maxEntries,
                                 File cacheDirectory) throws Exception
  {
    super(memberIdStart, memberIdCount, 4 /* elementSize */, maxEntrySize, maxEntries, cacheDirectory, new EntryIntFactory());
  }
  
  @Override
  protected void loadArrayFileData()
  {
    long maxScn = 0;
    
    try
    {
      maxScn = _arrayFile.readMaxSCN();
      _parallelData = _arrayFile.loadIntArray();
      if (_parallelData.length != _memberIdCount)
      {
        maxScn = 0;
        _parallelData = new int[_memberIdCount];
        clear();
        
        log.warn("Allocated _parallelData due to invalid length");
      }
      else
      {
        log.info("Data loaded successfully from file " + _arrayFile.getName());
      }
    }
    catch(Exception e)
    {
      maxScn = 0;
      _parallelData = new int[_memberIdCount];
      clear();
      
      log.warn("Allocated _parallelData due to a thrown exception: " + e.getMessage());
    }
    
    _entryManager.setWaterMarks(maxScn, maxScn);
  }
  
  /**
   * Sync-up the high water mark to a given value.
   * 
   * @param endOfPeriod
   */
  @Override
  public void saveHWMark(long endOfPeriod)
  {
    if (getHWMark() < endOfPeriod)
    {
      try
      {
        setData(getIndexStart(), getData(getIndexStart()), endOfPeriod);
      }
      catch(Exception e)
      {
        log.error(e);
      }
    }
  }
  
  public void clear()
  {
    if (_parallelData != null)
    {
      for (int i = 0; i < _parallelData.length; i ++)
      {
        _parallelData[i] = 0;
      }
    }
  }
  
  public int getData(int index)
  {
    return _parallelData[index - _memberIdStart];
  }
  
  public void setData(int index, int value, long scn) throws Exception
  {
    int pos = index - _memberIdStart;
    _parallelData[pos] = value;
    _entryManager.addToEntry(new EntryValueInt(pos, value, scn));  
  }
  
  @Override
  public Object memoryClone()
  {
      IntArrayMemoryImpl memClone = new IntArrayMemoryImpl(getIndexStart(), length());
      
      System.arraycopy(_parallelData, 0, memClone.getParallelData(), 0, _parallelData.length);
      memClone._lwmScn = getLWMark(); 
      memClone._hwmScn = getHWMark();
      
      return memClone;
  }
  
  public static class Config extends RecoverableArrayImpl.Config<EntryValueInt>
  {
    // super configuration class provides everything
  }
}
