package krati.cds.impl.array.basic;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import krati.cds.Persistable;
import krati.cds.array.Array;
import krati.cds.impl.array.entry.Entry;
import krati.cds.impl.array.entry.EntryFactory;
import krati.cds.impl.array.entry.EntryValue;

public abstract class RecoverableArray<V extends EntryValue> implements Array, Persistable
{
  private static final Logger _log = Logger.getLogger(RecoverableArray.class);
  
  protected int                  _length;         // Length of this array
  protected File                 _directory;      // array cache directory
  protected ArrayFile            _arrayFile;      // underlying array file
  protected EntryFactory<V>      _entryFactory;   // factory for creating Entry
  protected ArrayEntryManager<V> _entryManager;   // manager for entry redo logs
  
  /**
   * @param length
   *            Length of the array.
   * @param entrySize
   *            Maximum number of values per entry.
   * @param maxEntries
   *            Maximum number of entries before applying them.
   * @param directory
   *            Directory to store the array file and redo entries.
   */
  protected RecoverableArray(int length,
                             int elemSize,
                             int entrySize,
                             int maxEntries,
                             File directory,
                             EntryFactory<V> entryFactory) throws Exception
  {
    _length = length;
    _directory = directory;
    _entryFactory = entryFactory;
    _entryManager = new ArrayEntryManager<V>(this, maxEntries, entrySize);
    
    if (!_directory.exists())
    {
      _directory.mkdirs();
    }
    
    boolean newFile = true;
    File file = new File(directory, "indexes.dat");
    if (file.exists())
    {
      newFile = false;
    }
    
    _arrayFile = new ArrayFile(file, length, elemSize);
    _length = _arrayFile.getArrayLength();
    
    if (newFile)
    {
      initArrayFileData();
    }
    
    init();
    
    _log.info("length:" + _length +
             " entrySize:" + entrySize +
             " maxEntries:" + maxEntries +
             " directory:" + directory.getAbsolutePath() +
             " arrayFile:" + _arrayFile.getName());
  }
  
  /**
   * Loads data from the array file.
   */
  protected void init() throws IOException
  {
    try
    {
      long maxScn = _arrayFile.getMaxScn();
      long newScn = _arrayFile.getNewScn();
      if (newScn < maxScn)
      {
        throw new IOException(_arrayFile.getAbsolutePath() + " is corrupted: maxScn=" + maxScn + " newScn=" + newScn);
      }
      
      // Initialize entry manager and process entry files on disk if any.
      _entryManager.init(maxScn, newScn);
      
      // Load data from the array file on disk.
      loadArrayFileData();
    }
    catch (IOException e)
    {
      _log.error(e.getMessage(), e);
      throw e;
    }
  }
  
  protected void initArrayFileData()
  {
    // Subclasses need to initialize ArrayFile
  }
  
  protected abstract void loadArrayFileData();
  
  public File getDirectory()
  {
    return _directory;
  }
  
  public EntryFactory<V> getEntryFactory()
  {
    return _entryFactory;
  }
  
  public ArrayEntryManager<V> getEntryManager()
  {
    return _entryManager;
  }
  
  @Override
  public boolean hasIndex(int index)
  {
    return (0 <= index && index < _length);
  }
  
  @Override
  public int length()
  {
    return _length;
  }
  
  /**
   * Sync array file with all entry logs. The writer will be blocked until all entry logs are applied.
   */
  @Override
  public void sync() throws IOException
  {
    _entryManager.sync();
    _log.info("array saved: length=" + length());
  }
  
  /**
   * Persists this array.
   */
  @Override
  public void persist() throws IOException
  {
    _entryManager.persist();
    _log.info("array persisted: length=" + length());
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
  
  protected void updateArrayFile(List<Entry<V>> entryList) throws IOException
  {
    _arrayFile.update(entryList);
  }
}
