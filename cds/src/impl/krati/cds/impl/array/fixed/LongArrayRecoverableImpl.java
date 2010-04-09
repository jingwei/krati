package krati.cds.impl.array.fixed;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import krati.cds.array.LongArray;
import krati.cds.impl.array.entry.EntryLongFactory;
import krati.cds.impl.array.entry.EntryValueLong;

public class LongArrayRecoverableImpl extends RecoverableArrayImpl<long[], EntryValueLong> implements LongArray
{
  private static final Logger log = Logger.getLogger(LongArrayRecoverableImpl.class);
  
  public LongArrayRecoverableImpl(Config config) throws Exception
  {
    this(config.getMemberIdStart(),
         config.getMemberIdCount(),
         config.getMaxEntrySize(),
         config.getMaxEntries(),
         config.getCacheDirectory());
  }
  
  public LongArrayRecoverableImpl(int memberIdStart,
                                  int memberIdCount,
                                  int maxEntrySize,
                                  int maxEntries,
                                  File cacheDirectory) throws Exception
  {
    super(memberIdStart, memberIdCount, 8 /* elementSize */, maxEntrySize, maxEntries, cacheDirectory, new EntryLongFactory());
  }
  
  @Override
  protected void loadArrayFileData()
  {
    long maxScn = 0;
    
    try
    {
      maxScn = _arrayFile.readMaxSCN();
      _parallelData = _arrayFile.loadLongArray();
      if (_parallelData.length != _memberIdCount)
      {
        maxScn = 0;
        _parallelData = new long[_memberIdCount];
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
      _parallelData = new long[_memberIdCount];
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
  
  @Override
  public void clear()
  {
    // Clear in-memory array
    if (_parallelData != null)
    {
      for (int i = 0; i < _parallelData.length; i ++)
      {
        _parallelData[i] = 0;
      }
    }
    
    // Clear the entry manager
    _entryManager.clear();
    
    // Clear the underly array file
    try
    {
      _arrayFile.reset(_parallelData, _entryManager.getLWMark());
    }
    catch(IOException e)
    {
      log.error(e.getMessage(), e);
    }
  }
  
  @Override
  public long getData(int index)
  {
    return _parallelData[index - _memberIdStart];
  }
  
  @Override
  public void setData(int index, long value, long scn) throws Exception
  {
    int pos = index - _memberIdStart;
    _parallelData[pos] = value;
    _entryManager.addToEntry(new EntryValueLong(pos, value, scn));  
  }
  
  @Override
  public Object memoryClone()
  {
      LongArrayMemoryImpl memClone = new LongArrayMemoryImpl(getIndexStart(), length());
      
      System.arraycopy(_parallelData, 0, memClone.getParallelData(), 0, _parallelData.length);
      memClone._lwmScn = getLWMark(); 
      memClone._hwmScn = getHWMark();
      
      return memClone;
  }

  public void wrap(LongArray newArray) throws Exception
  {
     if(length() != newArray.length() || getIndexStart() != newArray.getIndexStart())
     {
         throw new ArrayIndexOutOfBoundsException();
     }
     
     _parallelData = newArray.getParallelData();
     _arrayFile.reset(_parallelData);
     _entryManager.clear();
  }
  
  public static class Config extends RecoverableArrayImpl.Config<EntryValueLong>
  {
    // super configuration class provides everything
  }
}
