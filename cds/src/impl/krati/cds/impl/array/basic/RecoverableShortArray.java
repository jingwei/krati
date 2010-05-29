package krati.cds.impl.array.basic;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import krati.cds.array.ShortArray;
import krati.cds.impl.array.entry.EntryShortFactory;
import krati.cds.impl.array.entry.EntryValueShort;

public class RecoverableShortArray extends RecoverableArray<EntryValueShort> implements ShortArray
{
  private static final Logger _log = Logger.getLogger(RecoverableShortArray.class);
  private short[] _internalArray;
  
  public RecoverableShortArray(int length,
                                   int entrySize,
                                   int maxEntries,
                                   File cacheDirectory) throws Exception
  {
    super(length, 2 /* elementSize */, entrySize, maxEntries, cacheDirectory, new EntryShortFactory());
  }
  
  @Override
  protected void loadArrayFileData()
  {
    long maxScn = 0;
    
    try
    {
      maxScn = _arrayFile.getMaxScn();
      _internalArray = _arrayFile.loadShortArray();
      if (_internalArray.length != _length)
      {
        maxScn = 0;
        _internalArray = new short[_length];
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
      _internalArray = new short[_length];
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
        set(0, get(0), endOfPeriod);
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
  
  public short get(int index)
  {
    return _internalArray[index];
  }
  
  public void set(int index, short value, long scn) throws Exception
  {
    _internalArray[index] = value;
    _entryManager.addToPreFillEntryShort(index, value, scn);  
  }
  
  @Override
  public short[] getInternalArray()
  {
    return _internalArray;
  }
}
