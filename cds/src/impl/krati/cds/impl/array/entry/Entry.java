package krati.cds.impl.array.entry;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import krati.io.ChannelReader;
import krati.io.DataWriter;
import krati.io.FastDataWriter;
import krati.util.Chronos;

/**
 * Entry.
 * 
 * Transactional Redo Entry.
 * 
 * @author jwu
 */
public class Entry<T extends EntryValue> implements Comparable<Entry<T>>
{
  public static final long STORAGE_VERSION = 0;
  private static final Logger _log = Logger.getLogger(Entry.class);
  
  private final int _initialCapacity;
  private final ArrayList<T> _valArray;
  private final EntryValueFactory<T> _valFactory;
  
  protected long _minScn = -1;
  protected long _maxScn = -1;
  
  /**
   * Create a new entry to hold updates to an array.
   * 
   * @param valFactory
   *        The factory for manufacturing EntryValue(s).
   * @param initialCapacity
   *        The initial number of values this entry can hold.
   */
  public Entry(EntryValueFactory<T> valFactory, int initialCapacity)
  {
    this._valFactory = valFactory;
    this._initialCapacity = initialCapacity;
    this._valArray = new ArrayList<T>(initialCapacity);
  }
  
  /**
   * Get the minimum SCN of updates maintained by this Entry.
   */
  public long getMinScn()
  {
    return _minScn;
  }
  
  /**
   * Get the maximum SCN of updates maintained by this Entry.
   */
  public long getMaxScn()
  {
    return _maxScn;
  }
  
  public int size()
  {
    return _valArray.size();
  }
  
  public boolean isFull()
  {
    return (_valArray.size() >= _initialCapacity);
  }
  
  public boolean isEmpty()
  {
    return _valArray.size() == 0;
  }
  
  /**
   * Checks if this entry can be switched.
   * 
   * An entry can be switched only if it is full and has seen more than one SCN. 
   */
  public boolean isSwitchable()
  {
    return ((_valArray.size() >= _initialCapacity) && (_minScn < _maxScn));
  }
  
  public void clear()
  {
    _valArray.clear();
    _minScn = -1;
    _maxScn = -1;
  }
  
  public void add(T value)
  {
    // add entry value
    _valArray.add(value);
    
    // set _minScn
    if (_minScn == -1) _minScn = value.scn;
    else _minScn = Math.min(_minScn, value.scn);
    
    // set _maxScn
    _maxScn = Math.max(_maxScn, value.scn);
  }
  
  /**
   * Merge all values from a given entry.
   * 
   * @param e entry to be merged
   */
  public void merge(Entry<T> e)
  {
    assert e != null;
    assert e._minScn != -1;
    assert e._minScn != -1;
    
    // Add all values
    for(T v : e._valArray)
    {
      _valArray.add(v);
    }
    
    // set _minScn
    if (_minScn == -1) _minScn = e.getMinScn();
    else _minScn = Math.min(_minScn, e.getMinScn());
    
    // set _maxScn
    _maxScn = Math.max(_maxScn, e.getMaxScn());
  }
  
  public int compareTo(Entry<T> e)
  {
    return (_maxScn < e._maxScn ? -1 : (_maxScn == e._maxScn ? 0 : 1));
  }
  
  public List<T> getValueList()
  {
    return _valArray;
  }
  
  public EntryValueFactory<T> getValueFactory()
  {
    return _valFactory;
  }
  
  /**
   * Saves to a file.
   * 
   * @param file
   * @throws IOException
   */
  public void save(File file) throws IOException
  {
    assert _maxScn != -1;
    assert _minScn != -1;
    
    Chronos c = new Chronos();
    
    // Load and merge the existing entry log if it exists.
    // Avoid data loss caused by log overwriting.
    if (file.exists())
    {
      try
      {
        load(file);
        _log.info("overwriting entry file " + file.getAbsolutePath());
      }
      catch(IOException e)
      {
        _log.warn("overwriting corrupted entry file " + file.getAbsolutePath());
      }
    }
    
    DataWriter out = new FastDataWriter(file);
    
    try
    {
      out.open();
      
      out.writeLong(STORAGE_VERSION); // write the entry file version
      out.writeLong(_minScn);
      out.writeLong(_maxScn);
      out.writeInt(_valArray.size());
      
      for (T val : _valArray)
      {
        val.write(out);
      }
      
      out.writeLong(_minScn);
      out.writeLong(_maxScn);
    }
    finally
    {
      out.close();
    }
    
    _log.info("Saved entry minScn:" + _minScn + " maxScn:" + _maxScn + " size:" + _valArray.size() + " file:" + file.getAbsolutePath() + " in " + c.getElapsedTime());
  }
  
  /**
   * Loads an entry from a given file.  
   * 
   * @param file
   * @throws IOException
   */
  public void load(File file) throws IOException
  {
    ChannelReader in = new ChannelReader(file);
    
    try
    {
      in.open();
      
      // Read entry head
      long fileVersion = in.readLong();
      if (fileVersion != STORAGE_VERSION)
      {
        throw new RuntimeException(
            "Wrong entry version " + fileVersion + " encounted in " + file.getAbsolutePath() +
            ". Version " + STORAGE_VERSION + " expected.");
      }
      
      long minScnHead = in.readLong();
      long maxScnHead = in.readLong();
      
      // Read number of EntryValue(s) in this Entry
      int length = in.readInt();
      
      // Read entry body
      for (int i = 0; i < length; i++)
      {
        _valArray.add(_valFactory.newValue(in));
      }
      
      // Read entry tail
      long minScnTail = in.readLong();
      long maxScnTail = in.readLong();
      
      if (minScnHead != minScnTail)
      {
        throw new IOException("min scns don't match: " + minScnHead + " vs " + minScnTail);
      }
      if (maxScnHead != maxScnTail)
      {
        throw new IOException("max scns don't match:" + maxScnHead + " vs " + maxScnTail);
      }
      
      _minScn = minScnHead;
      _maxScn = maxScnHead;
    }
    finally
    {
      in.close();
    }
  }
  
}
