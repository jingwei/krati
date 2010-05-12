package krati.cds.impl.array.basic;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import krati.cds.array.LongArray;
import krati.cds.impl.array.entry.EntryLongFactory;
import krati.cds.impl.array.entry.EntryValueLong;

/**
 * LongArrayRecoverableImpl: Simple Persistent LongArray Implementation.
 * 
 * This class is not thread-safe by design. It is expected that the conditions below hold within one JVM.
 * <pre>
 *    1. There is one and only one instance of LongArrayRecoverableImpl for a given cacheDirectory.
 *    2. There is one and only one thread is calling the setData method at any given time. 
 * </pre>
 * 
 * It is expected that this class is used in the case of multiple readers and single writer.
 * 
 * @author jwu
 *
 */
public class LongArrayRecoverableImpl extends RecoverableArrayImpl<long[], EntryValueLong> implements LongArray
{
  private static final Logger _log = Logger.getLogger(LongArrayRecoverableImpl.class);
  
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
      _internalArray = _arrayFile.loadLongArray();
      if (_internalArray.length != _memberIdCount)
      {
        maxScn = 0;
        _internalArray = new long[_memberIdCount];
        clear();
        
        _log.warn("Allocated _internalArray due to invalid length");
      }
      else
      {
        _log.info("Data loaded successfully from file " + _arrayFile.getName());
      }
    }
    catch(Exception e)
    {
      maxScn = 0;
      _internalArray = new long[_memberIdCount];
      clear();
      
      _log.warn("Allocated _internalArray due to a thrown exception: " + e.getMessage());
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
        _log.error(e);
      }
    }
  }
  
  @Override
  public void clear()
  {
    // Clear in-memory array
    if (_internalArray != null)
    {
      for (int i = 0; i < _internalArray.length; i ++)
      {
        _internalArray[i] = 0;
      }
    }
    
    // Clear the entry manager
    _entryManager.clear();
    
    // Clear the underly array file
    try
    {
      _arrayFile.reset(_internalArray, _entryManager.getLWMark());
    }
    catch(IOException e)
    {
      _log.error(e.getMessage(), e);
    }
  }
  
  @Override
  public long getData(int index)
  {
    return _internalArray[index - _memberIdStart];
  }
  
  @Override
  public void setData(int index, long value, long scn) throws Exception
  {
    int pos = index - _memberIdStart;
    _internalArray[pos] = value;
    _entryManager.addToEntry(new EntryValueLong(pos, value, scn));  
  }
  
  @Override
  public Object memoryClone()
  {
      LongArrayMemoryImpl memClone = new LongArrayMemoryImpl(getIndexStart(), length());
      
      System.arraycopy(_internalArray, 0, memClone.getInternalArray(), 0, _internalArray.length);
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
     
     _internalArray = newArray.getInternalArray();
     _arrayFile.reset(_internalArray);
     _entryManager.clear();
  }
  
  public static class Config extends RecoverableArrayImpl.Config<EntryValueLong>
  {
    // super configuration class provides everything
  }
}
