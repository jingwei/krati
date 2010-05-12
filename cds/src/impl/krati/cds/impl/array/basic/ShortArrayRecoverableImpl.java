package krati.cds.impl.array.basic;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import krati.cds.array.ShortArray;
import krati.cds.impl.array.entry.EntryShortFactory;
import krati.cds.impl.array.entry.EntryValueShort;

public class ShortArrayRecoverableImpl extends RecoverableArrayImpl<short[], EntryValueShort> implements ShortArray
{
  private static final Logger _log = Logger.getLogger(ShortArrayRecoverableImpl.class);
  
  public ShortArrayRecoverableImpl(Config config) throws Exception
  {
    this(config.getMemberIdStart(),
         config.getMemberIdCount(),
         config.getMaxEntrySize(),
         config.getMaxEntries(),
         config.getCacheDirectory());
  }
  
  public ShortArrayRecoverableImpl(int memberIdStart,
                                   int memberIdCount,
                                   int maxEntrySize,
                                   int maxEntries,
                                   File cacheDirectory) throws Exception
  {
    super(memberIdStart, memberIdCount, 2 /* elementSize */, maxEntrySize, maxEntries, cacheDirectory, new EntryShortFactory());
  }
  
  @Override
  protected void loadArrayFileData()
  {
    long maxScn = 0;
    
    try
    {
      maxScn = _arrayFile.readMaxSCN();
      _internalArray = _arrayFile.loadShortArray();
      if (_internalArray.length != _memberIdCount)
      {
        maxScn = 0;
        _internalArray = new short[_memberIdCount];
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
      _internalArray = new short[_memberIdCount];
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
  
  public short getData(int index)
  {
    return _internalArray[index - _memberIdStart];
  }
  
  public void setData(int index, short value, long scn) throws Exception
  {
    int pos = index - _memberIdStart;
    _internalArray[pos] = value;
    _entryManager.addToEntry(new EntryValueShort(pos, value, scn));  
  }
  
  public static class Config extends RecoverableArrayImpl.Config<EntryValueShort>
  {
    // super configuration class provides everything
  }
}
