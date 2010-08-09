package krati.cds.impl.array.basic;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import krati.cds.impl.array.entry.Entry;
import krati.cds.impl.array.entry.EntryFactory;
import krati.cds.impl.array.entry.EntryValue;

public abstract class AbstractRecoverableArray<V extends EntryValue> implements RecoverableArray<V>
{
  static final Logger _log = Logger.getLogger(AbstractRecoverableArray.class);
  
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
  protected AbstractRecoverableArray(int length,
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
    
    File file = new File(directory, "indexes.dat");
    _arrayFile = openArrayFile(file, length, elemSize);
    _length = _arrayFile.getArrayLength();
    
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
      long lwmScn = _arrayFile.getLwmScn();
      long hwmScn = _arrayFile.getHwmScn();
      if (hwmScn < lwmScn)
      {
        throw new IOException(_arrayFile.getAbsolutePath() + " is corrupted: lwmScn=" + lwmScn + " hwmScn=" + hwmScn);
      }
      
      // Initialize entry manager and process entry files on disk if any.
      _entryManager.init(lwmScn, hwmScn);
      
      // Load data from the array file on disk.
      loadArrayFileData();
    }
    catch (IOException e)
    {
      _log.error(e.getMessage(), e);
      throw e;
    }
  }
  
  protected final ArrayFile openArrayFile(File file, int initialLength, int elementSize) throws IOException
  {
      boolean isNew = true;
      if(file.exists()) isNew = false;
      
      ArrayFile arrayFile = new ArrayFile(file, initialLength, elementSize);
      if(isNew) initArrayFile();
      
      return arrayFile;
  }
  
  protected void initArrayFile()
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
  
  @Override
  public void updateArrayFile(List<Entry<V>> entryList) throws IOException
  {
    _arrayFile.update(entryList);
  }
}
