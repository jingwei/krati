package krati.cds.impl.array.fixed;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import krati.cds.Persistable;
import krati.cds.impl.array.entry.Entry;
import krati.cds.impl.array.entry.EntryFactory;
import krati.cds.impl.array.entry.EntryPersistListener;
import krati.cds.impl.array.entry.EntryValue;

public class ArrayEntryManager<V extends EntryValue> implements Persistable
{
  private static final Logger _log = Logger.getLogger(ArrayEntryManager.class);
  
  protected final int _maxEntries;
  protected final int _maxEntrySize;
  protected volatile boolean _autoSwitchEntry = true;    // Automatically switch to a new entry if _curEntry is full
  protected volatile boolean _autoApplyEntries = true;   // Automatically apply accumulated entries to the array file
  protected volatile long _lwmScn = 0;                   // Low water mark SCN starts from 0
  protected volatile long _hwmScn = 0;                   // High water mark SCN starts from 0
  
  protected RecoverableArray<V>  _array;
  protected List<Entry<V>>       _entryList;      // list of redo entry(s)
  protected List<Entry<V>>       _clonedEntryList;// list of cloned redo entry(s)
  protected Entry<V>             _currentEntry;   // current redo entry
  protected EntryPersistListener _persistListener;
  
  public ArrayEntryManager(RecoverableArray<V> array, int maxEntries, int maxEntrySize)
  {
    this._array = array;
    this._maxEntries = maxEntries;
    this._maxEntrySize = maxEntrySize;
    this._entryList = new ArrayList<Entry<V>>(maxEntries + 5);
    this._clonedEntryList = new ArrayList<Entry<V>>(maxEntries + 5);
    
    _log.info("Manage array indexStart=" + array.getIndexStart() + " length=" + array.length());
  }
  
  public int getMaxEntries()
  {
    return _maxEntries;
  }
  
  public int getMaxEntrySize()
  {
    return _maxEntrySize;
  }
  
  public File getCacheDirectory()
  {
    return _array.getCacheDirectory();
  }
  
  public EntryFactory<V> getEntryFactory()
  {
    return _array.getEntryFactory();
  }
  
  public boolean getAutoSwitchEntry()
  {
    return _autoSwitchEntry;
  }
  
  public void setAutoSwitchEntry(boolean  b)
  {
    _autoSwitchEntry = b;
  }
  
  public boolean getAutoApplyEntries()
  {
    return _autoApplyEntries;
  }
  
  public void setAutoApplyEntries(boolean b)
  {
    _autoApplyEntries = b;
  }
  
  public void addToEntry(V entryValue) throws IOException
  {
    if (_currentEntry == null)
    {
      _currentEntry = createEntry();
    }
    
    // Add to current entry
    _currentEntry.add(entryValue);
    
    // Advance high water mark to maintain progress record
    _hwmScn = Math.max(_hwmScn, entryValue.scn);
    
    // Switch to a new entry if _currentEntry has reached _maxEntrySize and has seen more than one SCN.
    if (_autoSwitchEntry && _currentEntry.isSwitchable())
    {
      switchEntry(false);
    }
  }
  
  public synchronized void clear()
  {
    _lwmScn = 0;
    _hwmScn = 0;
    _entryList.clear();
    _currentEntry = null;
    
    try
    {
        deleteEntryFiles();
    }
    catch(IOException e)
    {
        _log.warn(e.getMessage());
    }
    
    _log.info("entry files cleared");
  }
  
  @Override
  public long getHWMark()
  {
    return _hwmScn;
  }
  
  @Override
  public long getLWMark()
  {
    return _lwmScn;
  }
  
  @Override
  public void saveHWMark(long endOfPeriod) throws Exception
  {
    _hwmScn = Math.max(_hwmScn, endOfPeriod);
  }
  
  public void setWaterMarks(long lwmScn, long hwmScn)
  {
    if (lwmScn <= hwmScn)
    {
      _lwmScn = lwmScn;
      _hwmScn = hwmScn;
    }
  }
  
  @Override
  public void persist() throws IOException
  {
    /* ************************* *
     * Run in non-blocking mode  *
     * ************************* */
    switchEntry(false);
  }
  
  public void setEntryPersistListener(EntryPersistListener listener)
  {
    this._persistListener = listener;
  }
  
  public EntryPersistListener getEntryPersistListener()
  {
    return _persistListener;
  }
  
  public File getEntryLogFile(String tag)
  {
    // trentry_<id>_<tag>.log
    return new File(getCacheDirectory(), getEntryLogPrefix() + "_" + tag + getEntryLogSuffix());
  }
  
  /**
   * Create a new entry.
   * @return a new entry.
   */
  protected Entry<V> createEntry()
  {
    return getEntryFactory().newEntry(_maxEntrySize);
  }
  
  /**
   * @return the name of entry log file.
   */
  protected final String getEntryLogName(Entry<V> entry)
  {
    return getEntryLogPrefix() + "_" + entry.getMinScn() + "_" + entry.getMaxScn() + getEntryLogSuffix();
  }
  
  /**
   * @return the prefix of entry log file.
   */
  protected final String getEntryLogPrefix()
  {
    return "trentry_" + _array.getIndexStart();
  }
  
  /**
   * @return the suffix of entry log file.
   */
  protected final String getEntryLogSuffix()
  {
    return ".log";
  }
  
  /**
   * @return a list of accumulated entries.
   */
  protected List<Entry<V>> getEntryList()
  {
    return _entryList;
  }
  
  /**
   * Switches to a new entry if _curEntry is not empty.
   * 
   * @throws IOException
   */
  protected synchronized void switchEntry(boolean blocking) throws IOException
  {
    if (_currentEntry != null && !_currentEntry.isEmpty())
    {
      if(_persistListener != null)
      {
          _persistListener.priorPersisting(_currentEntry);
      }

      // Create entry log and persist in-memory data
      File file = new File(getCacheDirectory(), getEntryLogName(_currentEntry));
      _currentEntry.save(file);
      
      if(_persistListener != null)
      {
          _persistListener.afterPersisting(_currentEntry);
      }
      
      // Advance low water mark to maintain progress record
      _lwmScn = Math.max(_lwmScn, _currentEntry.getMaxScn());
      _entryList.add(_currentEntry);
      _currentEntry = null;
      
      _log.info("_lwmScn=" + _lwmScn + " _hwmScn=" + _hwmScn);
    }
    
    // Apply entry logs to array file
    if (_autoApplyEntries)
    {
      if (_entryList.size() >= _maxEntries)
      {
        applyEntries(blocking);
      }
    }
  }
  
  /**
   * Apply accumulated entries to the array file.
   * @throws IOException
   */
  protected synchronized void applyEntries(boolean blocking) throws IOException
  {
    if (_entryList.size() > 0)
    {
      synchronized(_clonedEntryList)
      {
        while(_clonedEntryList.size() > 0)
        {
          try
          { 
            _clonedEntryList.wait();
          }
          catch(InterruptedException ie)
          {
            /* Run in blocking mode */
            
            // Update underlying array file
            _array.updateArrayFile(_entryList);
              
            // Clean up entry files
            deleteEntryFiles(_entryList);
            _entryList.clear();
            return;
          }
        }
        
        // Blocking mode
        if (blocking)
        {
          // Update underlying array file
          _array.updateArrayFile(_entryList);
          
          // Clean up entry files
          deleteEntryFiles(_entryList);
          _entryList.clear();
        }
        // Non-blocking mode
        else
        {
          // Start a separate thread to update the underlying array file
          for(Entry<V> e: _entryList) { _clonedEntryList.add(e); }
          new Thread(new UpdateRunner()).run();
          _entryList.clear();
        }
      }
    }
  }
  
  class UpdateRunner implements Runnable
  { 
    @Override
    public void run()
    {
      try
      {
        synchronized(_clonedEntryList)
        {
          // Update underlying array file
          _array.updateArrayFile(_clonedEntryList);
          
          // Clean up entry files
          deleteEntryFiles(_clonedEntryList);
          _clonedEntryList.clear();  
          _clonedEntryList.notifyAll();
        }
      }
      catch(IOException ioe)
      {
        _log.error(ioe.getMessage());
      }
    }
  }
  
  /**
   * Load entry log files from disk into _entryList.
   * 
   * @throws IOException
   */
  protected void loadEntryFiles()
  {
    File[] files = getCacheDirectory().listFiles();
    String prefix = getEntryLogPrefix();
    String suffix = getEntryLogSuffix();
    
    for (File file : files)
    {
      String fileName = file.getName();
      if (fileName.startsWith(prefix) && fileName.endsWith(suffix))
      {
        try
        {
          Entry<V> entry = createEntry();
          entry.load(file);
          _entryList.add(entry);
        }
        catch(IOException e)
        {
          String filePath = file.getAbsolutePath();
          _log.warn(filePath + " corrupted");
          if(file.delete())
          {
            _log.warn(filePath + " deleted");
          }
        }
      }
    }
  }
  
  /**
   * Delete entry log files on disk.
   * 
   * @throws IOException
   */
  protected void deleteEntryFiles() throws IOException
  {
    File[] files = getCacheDirectory().listFiles();
    String prefix = getEntryLogPrefix();
    String suffix = getEntryLogSuffix();
    
    for (File file : files)
    {
      String fileName = file.getName();
      if (fileName.startsWith(prefix) && fileName.endsWith(suffix))
      {
        if (file.delete())
        {
          _log.info("file " + file.getAbsolutePath() + " deleted");
        }
        else 
        {
          _log.warn("file " + file.getAbsolutePath() + " not deleted");
        }
      }
    }
  }
  
  /**
   * Delete entry log files on disk.
   * 
   * @throws IOException
   */
  protected void deleteEntryFiles(List<Entry<V>> list) throws IOException
  {
    for(Entry<V> e: list)
    {
      File file = new File(getCacheDirectory(), getEntryLogName(e));
      if(file.exists())
      {
        if (file.delete())
        {
          _log.info("file " + file.getAbsolutePath() + " deleted");
        }
        else 
        {
          _log.warn("file " + file.getAbsolutePath() + " not deleted");
        }
      }
    }
  }
  
  /**
   * Filter and select entries in _entryList
   * that only have SCNs no less than the specified lower bound minScn and
   * that only have SCNs no greater than the specified upper bound maxScn.
   * 
   * @param minScn Inclusive lower bound SCN.
   * @param maxScn Inclusive upper bound SCN.
   */
  protected void filterEntryList(long minScn, long maxScn)
  {
    List<Entry<V>> result = new ArrayList<Entry<V>>(_entryList.size());
    for (Entry<V> e : _entryList)
    {
      if (minScn <= e.getMinScn() && e.getMaxScn() <= maxScn)
      {
        result.add(e);
      }
    }
    
    if (result.size() < _entryList.size())
    {
      _entryList.clear();
      _entryList.addAll(result);
    }
  }
  
  /**
   * Filter and select entries in _entryList that only have SCNs no less than the specified lower bound.
   * 
   * @param scn Inclusive lower bound SCN
   */
  protected void filterEntryListLowerBound(long scn)
  {
    List<Entry<V>> result = new ArrayList<Entry<V>>(_entryList.size());
    for (Entry<V> e : _entryList)
    {
      if (scn <= e.getMinScn())
      {
        result.add(e);
      }
    }
    
    if (result.size() < _entryList.size())
    {
      _entryList.clear();
      _entryList.addAll(result);
    }
  }

  /**
   * Filter and select entries in _entryList that only have SCNs no greater than the specified upper bound.
   * 
   * @param scn Inclusive upper bound SCN
   */
  protected void filterEntryListUpperBound(long scn)
  {
    List<Entry<V>> result = new ArrayList<Entry<V>>(_entryList.size());
    for (Entry<V> e : _entryList)
    {
      if (e.getMaxScn() <= scn)
      {
        result.add(e);
      }
    }
    
    if (result.size() < _entryList.size())
    {
      _entryList.clear();
      _entryList.addAll(result);
    }
  }

}
