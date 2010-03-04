package krati.mds.impl.array.fixed;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import krati.mds.impl.array.entry.Entry;
import krati.mds.impl.array.entry.EntryFactory;
import krati.mds.impl.array.entry.EntryValue;
import krati.mds.parallel.AbstractParallelDataStore;

public abstract class RecoverableArrayImpl<P, V extends EntryValue>
       extends AbstractParallelDataStore<P> implements RecoverableArray<V>
{
  private static final Logger log = Logger.getLogger(RecoverableArrayImpl.class);
  
  protected final File            _cacheDirectory; // cache directory for entry redo logs
  protected EntryFactory<V>       _entryFactory;   // factory for creating Entry
  protected ArrayRandomAccessFile _arrayFile;      // underlying array file
  protected ArrayEntryManager<V>  _entryManager;   // manager for entry redo logs
  
  /**
   * @param config
   */
  public RecoverableArrayImpl(Config<V> config) throws Exception
  {
    this(config.getMemberIdStart(),
         config.getMemberIdCount(),
         config.getElementSize(),
         config.getMaxEntrySize(),
         config.getMaxEntries(),
         config.getCacheDirectory(),
         config.getEntryFactory());
  }
  
  /**
   * @param memberIdStart
   *            Start index of the array 
   * @param memberIdCount
   *            Number of members in the array
   * @param maxEntrySize
   *            Max values per entry
   * @param maxEntries
   *            Maximum number of entries before merging them
   * @param cacheDirectory
   *            Directory to store the files
   */
  public RecoverableArrayImpl(int memberIdStart,
                              int memberIdCount,
                              int elementSize,
                              int maxEntrySize,
                              int maxEntries,
                              File cacheDirectory,
                              EntryFactory<V> entryFactory) throws Exception
  {
    super(memberIdStart, memberIdCount);
    _cacheDirectory = cacheDirectory;
    _entryFactory = entryFactory;
    _entryManager = new ArrayEntryManager<V>(this, maxEntries, maxEntrySize);
    
    if (!_cacheDirectory.exists())
    {
      _cacheDirectory.mkdirs();
    }
    
    boolean newFile = true;
    File file = new File(cacheDirectory, "parallel_" + getMemberIdStart() + "_" + getMemberIdCount() + ".dat");
    if (file.exists())
    {
      newFile = false;
    }
    
    _arrayFile = new ArrayRandomAccessFile(file, _memberIdCount, elementSize);
    
    if (newFile)
    {
      initArrayFileData();
    }
    
    log.info("indexStart:" + memberIdStart +
             " length:" + memberIdCount +
             " maxEntrySize:" + maxEntrySize +
             " maxEntries:" + maxEntries +
             " cacheDirectory:" + cacheDirectory.getAbsolutePath() +
             " arrayFile:" + _arrayFile.getName());
    
    init();
  }
  
  /**
   * Loads data from the array file.
   */
  @Override
  protected void init() 
  { 
    // Avoid exception in super()
    if(_arrayFile == null) return;
    
    try
    {
      long maxSCN = _arrayFile.readMaxSCN();
      long copySCN = _arrayFile.readCopySCN();
      if (copySCN < maxSCN)
      {
        throw new IOException(_arrayFile.getAbsolutePath() + " is corrupted: maxSCN=" + maxSCN + " copySCN=" + copySCN);
      }
      
      // Load entries from logs on disk 
      _entryManager.loadEntryFiles();
      
      // Sanitize loaded entries
      if (maxSCN == copySCN) // array file is consistent 
      {
        // Find entries that have not been flushed to the array file
        if(_entryManager.getEntryList().size() > 0)
        {
          _entryManager.filterEntryListLowerBound(maxSCN);
        }
      }
      else                   // array file is inconsistent
      {
        if(_entryManager.getEntryList().size() > 0)
        {
          _entryManager.filterEntryList(maxSCN, copySCN);
          
          if(_entryManager.getEntryList().size() == 0)
          {
            _entryManager.deleteEntryFiles();
            log.error("entry files for recovery not found");
          }
        }
      }
      
      // Start recovery based on loaded entries
      if (_entryManager.getEntryList().size() > 0)
      {
        _entryManager.applyEntries();
      }
      
      // Load data from the array file on disk
      loadArrayFileData();
    }
    catch (IOException ioe)
    {
      log.error(ioe.getMessage());
      
      // Load data from the array file on disk, which may not contain correct data
      loadArrayFileData();
    }
  }
  
  protected void initArrayFileData()
  {
    // Subclasses need to initialize parallel data in ArrayFile
  }
  
  protected abstract void loadArrayFileData();
  
  public File getCacheDirectory()
  {
    return _cacheDirectory;
  }

  public EntryFactory<V> getEntryFactory()
  {
    return _entryFactory;
  }
  
  public ArrayEntryManager<V> getEntryManager()
  {
    return _entryManager;
  }
  
  public int getIndexStart()
  {
    return _memberIdStart;
  }
  
  public boolean indexInRange(int index)
  {
    return (_memberIdStart <= index && index < _memberIdEnd);
  }
  
  public int length()
  {
    return _memberIdCount;
  }
  
  /**
   * Flushes the current entry to log file.
   */
  public void flush() throws IOException
  {
    _entryManager.switchEntry();
  }
  
  /**
   * Persists this array.
   */
  @Override
  public void persist() throws IOException
  {
    _entryManager.persist();
    log.info("array persisted: indexStart=" + getIndexStart());
  }
  
  @Override
  public long getHWMark()
  {
    return _entryManager.getHWMark();
  }
  
  @Override
  public long getLWMark()
  {
    return _entryManager.getLWMark();
  }
  
  @Override
  public void updateArrayFile(List<Entry<V>> entryList) throws IOException
  {
    _arrayFile.update(entryList);
  }
  
  /**
   * Inner class for configuration.
   * 
   * @author jwu
   *
   * @param <T> Basic value contained in a redo entry.
   */
  public static class Config<T extends EntryValue> extends AbstractParallelDataStore.Config
  {
    private EntryFactory<T> _entryFactory;
    private File            _cacheDirectory;
    private int             _elementSize;
    private int             _maxEntrySize;
    private int             _maxEntries;
    
    public void setElementSize(int elementSize)
    {
      this._elementSize = elementSize;
    }
    
    public int getElementSize()
    {
      return _elementSize;
    }
    
    public void setMaxEntrySize(int maxEntrySize)
    {
      this._maxEntrySize = maxEntrySize;
    }

    public int getMaxEntrySize()
    {
      return _maxEntrySize;
    }
    
    public void setMaxEntries(int maxEntries)
    {
      this._maxEntries = maxEntries;
    }
    
    public int getMaxEntries()
    {
      return _maxEntries;
    }

    public void setEntryFactory(EntryFactory<T> entryFactory)
    {
      this._entryFactory = entryFactory;
    }
    
    public EntryFactory<T> getEntryFactory()
    {
      return _entryFactory;
    }
    
    /**
     * @return the cache directory
     */
    public File getCacheDirectory()
    {
      return _cacheDirectory;
    }
    
    /**
     * @param cacheDirectory the cache directory to set
     */
    public void setCacheDirectory(File cacheDirectory)
    {
      _cacheDirectory = cacheDirectory;
    }
  }

}
