package krati.cds.impl.array.basic;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import krati.cds.array.LongArray;
import krati.cds.impl.array.AddressArray;
import krati.cds.impl.array.entry.EntryLongFactory;
import krati.cds.impl.array.entry.EntryPersistListener;
import krati.cds.impl.array.entry.EntryValueLong;

/**
 * SimpleLongArray: Simple Persistent LongArray Implementation.
 * 
 * This class is not thread-safe by design. It is expected that the conditions below hold within one JVM.
 * <pre>
 *    1. There is one and only one instance of SimpleLongArray for a given home directory.
 *    2. There is one and only one thread is calling the setData method at any given time. 
 * </pre>
 * 
 * It is expected that this class is used in the case of multiple readers and single writer.
 * 
 * @author jwu
 *
 */
public class SimpleLongArray extends AbstractRecoverableArray<EntryValueLong> implements AddressArray
{
  private static final Logger _log = Logger.getLogger(SimpleLongArray.class);
  private long[] _internalArray;
  
  /**
   * Create a fixed-length persistent long array.
   * 
   * @param length         the length of this array
   * @param entrySize      the size of redo entry (i.e., batch size) 
   * @param maxEntries     the number of redo entries required for updating the underlying array file
   * @param homeDirectory  the home directory of this array
   * @throws Exception     if this array cannot be created.
   */
  public SimpleLongArray(int length, int entrySize, int maxEntries, File homeDirectory) throws Exception
  {
    super(length, 8 /* elementSize */, entrySize, maxEntries, homeDirectory, new EntryLongFactory());
  }
  
  @Override
  protected void loadArrayFileData()
  {
    long maxScn = 0;
    
    try
    {
      maxScn = _arrayFile.getLwmScn();
      _internalArray = _arrayFile.loadLongArray();
      if (_internalArray.length != _length)
      {
        maxScn = 0;
        _internalArray = new long[_length];
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
      _internalArray = new long[_length];
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
  public long get(int index)
  {
    return _internalArray[index];
  }
  
  @Override
  public void set(int index, long value, long scn) throws Exception
  {
    _internalArray[index] = value;
    _entryManager.addToPreFillEntryLong(index, value, scn);  
  }
  
  @Override
  public long[] getInternalArray()
  {
    return _internalArray;
  }
  
  public void wrap(LongArray newArray) throws Exception
  {
     if(length() != newArray.length())
     {
         throw new ArrayIndexOutOfBoundsException();
     }
     
     _internalArray = newArray.getInternalArray();
     _arrayFile.reset(_internalArray);
     _entryManager.clear();
  }

  @Override
  public EntryPersistListener getPersistListener()
  {
    return getEntryManager().getEntryPersistListener();
  }

  @Override
  public void setPersistListener(EntryPersistListener persistListener)
  {
      getEntryManager().setEntryPersistListener(persistListener);
  }
}
