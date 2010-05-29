package krati.cds.impl.array;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import krati.cds.Persistable;
import krati.cds.array.DynamicArray;
import krati.cds.array.IntArray;
import krati.cds.impl.array.basic.RecoverableIntArray;

public class DynamicIntArrayImpl implements IntArray, DynamicArray, Persistable
{
  protected static final Logger _log = Logger.getLogger(DynamicIntArrayImpl.class);

  protected long _lwmScn = 0;
  protected long _hwmScn = 0;
  protected int[][] _dataArrays = new int[0][0];
  protected RecoverableIntArray[] _implArrays = new RecoverableIntArray[0];
  
  protected final File _cacheDirectory;
  protected final int _maxEntrySize;
  protected final int _maxEntries;
  protected final int _subArrayBits;
  protected final int _subArraySize;
  protected final int _subArrayMask;
  
  public DynamicIntArrayImpl(Config config) throws Exception
  {
    this(config.getCacheDirectory(),
         config.getSubArrayBits(),
         config.getMaxEntrySize(),
         config.getMaxEntries());
  }
  
  public DynamicIntArrayImpl(File cacheDirectory,
                             int subArrayBits,
                             int maxEntrySize,
                             int maxEntries) throws Exception
  {
    this._cacheDirectory = cacheDirectory;
    this._subArrayBits = subArrayBits;
    this._maxEntrySize = maxEntrySize;
    this._maxEntries = maxEntries;
    this._subArraySize = 1 << subArrayBits;
    this._subArrayMask = this._subArraySize - 1;
    this.loadCache();
  }
  
  protected void loadCache() throws Exception
  {
    // Make sure cacheDirectory exists
    if (!_cacheDirectory.exists())
    {
      _cacheDirectory.mkdirs();
    }
    
    _log.info("start to load array ... _subArraySize="+_subArraySize);
    
    String prefix = "indexes_";
    String suffix = _subArraySize + ".dat";
    File[] files = _cacheDirectory.listFiles();
    
    // Find the maximum indexStart of all sub-arrays
    int indexStart = 0;
    boolean foundIndexes = false;
    for (File file : files)
    {
      String fileName = file.getName();
      if (fileName.startsWith(prefix) && fileName.endsWith(suffix))
      {
        _log.info("found indexes " + fileName);
        
        int fromIndex = fileName.indexOf('_');
        int endIndex = fileName.indexOf('_', fromIndex + 1);
        if (fromIndex < endIndex)
        {
          String num = fileName.substring(fromIndex + 1, endIndex);
          try
          {
            indexStart = Math.max(indexStart, Integer.parseInt(num));
            _log.info("indexes data index starts at " + indexStart);
            foundIndexes = true;
          }
          catch(Exception e)
          {
            _log.error(e.getMessage());
          }
        }
      }
    }
    
    if (foundIndexes)
    {
      // Expand capacity to include the maximum indexStart of sub-arrays
      this.expandCapacity(indexStart);
      
      // Calculate _hwmScn by finding the smallest _hwmScn of all sub-arrays
      for(RecoverableIntArray implArray : _implArrays)
      {
        long implHwmScn = implArray.getHWMark();
        if(implHwmScn > 0)
        {
          _hwmScn = (_hwmScn == 0) ? implHwmScn : Math.min(_hwmScn, implHwmScn);
        }
      }
      
      _lwmScn = getLWMark();
      _log.info("array loaded successfully: _lwmScn=" + _lwmScn + " _hwmScn=" + _hwmScn);
    }
  }
  
  /**
   * @return the current length of this IntArray
   */
  public int length()
  {
    return _dataArrays.length * _subArraySize;
  }
  
  /**
   * @return a boolean indicating an index is in the current range of this IntArray.
   */
  public boolean hasIndex(int index)
  {
    return (index >> _subArrayBits) < _dataArrays.length;
  }
  
  public int get(int index)
  {
    int segInd = index >> _subArrayBits;
    int subInd = index & _subArrayMask;
    
    return _dataArrays[segInd][subInd];
  }
  
  public void set(int index, int value, long scn) throws Exception
  {
    int segInd = index >> _subArrayBits;
    int subInd = index & _subArrayMask;
    
    // Expand array capacity automatically
    if (segInd >= _implArrays.length)
    {
      try
      {
        expandCapacity(index);
      }
      catch(Exception e)
      {
        _log.warn(e.getMessage());
      }
    }
    
    _implArrays[segInd].set(subInd, value, scn);
    _hwmScn = Math.max(_hwmScn, scn);
  }
  
  public synchronized void expandCapacity(int index) throws Exception
  {
    int numSubArrays = (index >> _subArrayBits) + 1;
    if (numSubArrays <= _implArrays.length)
    {
      return;
    }
    
    RecoverableIntArray[] tempArrays = new RecoverableIntArray[numSubArrays];
    
    int i = 0;
    for (; i < _implArrays.length; i++)
    {
      tempArrays[i] = _implArrays[i];
    }
    
    for(; i < numSubArrays; i++)
    {
      tempArrays[i] =  new RecoverableIntArray(_subArraySize,
                                                   _maxEntrySize,
                                                   _maxEntries,
                                                   _cacheDirectory);
    }
    
    _implArrays = tempArrays;
    
    int[][] tempDataArrays = new int[tempArrays.length][];
    for(i = 0; i < _implArrays.length; i++)
    {
      tempDataArrays[i] = tempArrays[i].getInternalArray();
    }
    _dataArrays = tempDataArrays;
  }
  
  @Override
  public long getLWMark()
  {
    long mark = 0;
    for(RecoverableIntArray implArray : _implArrays)
    {
      mark = (mark == 0) ? implArray.getLWMark() : Math.min(mark, implArray.getLWMark());
    }
    
    _lwmScn = mark;
    return _lwmScn;
  }
  
  @Override
  public long getHWMark()
  {
    return _hwmScn;
  }
  
  @Override
  public void saveHWMark(long endOfPeriod)
  {
    long mark = Math.max(_hwmScn, endOfPeriod);
    for(RecoverableIntArray implArray : _implArrays)
    {
      implArray.saveHWMark(mark);
    }
    _hwmScn = mark;
  }
  
  @Override
  public void sync() throws IOException
  {
    // Sync high-water marks
    saveHWMark(getHWMark());
    
    // Persist each sub-array
    for(RecoverableIntArray implArray : _implArrays)
    {
      try
      {
        implArray.sync();
      }
      catch(IOException e)
      {
        _log.error(e.getMessage());
      }
    }
  }
  
  @Override
  public void persist() throws IOException
  {
    // Sync high-water marks
    saveHWMark(getHWMark());
    
    // Persist each sub-array
    for(RecoverableIntArray implArray : _implArrays)
    {
      try
      {
        implArray.persist();
      }
      catch(IOException e)
      {
        _log.error(e.getMessage());
      }
    }
  }
  
  public void clear()
  {
    for(RecoverableIntArray implArray : _implArrays)
    {
      implArray.clear();
    }
  }
  
  public int[] getInternalArray()
  {
    int[] result = new int[length()];
    for(int i = 0; i < _dataArrays.length; i++)
    {
      System.arraycopy(_dataArrays[i], 0, result, i * _subArraySize, _subArraySize);
    }
    
    return result;
  }
  
  
  
  /**
   * Inner class for configuration.
   * 
   * @author jwu
   */
  public static class Config {
    private File _cacheDirectory;
    private int _subArrayBits;
    private int _maxEntrySize;
    private int _maxEntries;
    
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
      this._cacheDirectory = cacheDirectory;
    }
    
    public int getSubArrayBits()
    {
      return this._subArrayBits;
    }
    
    public void setSubArrayBits(int subArrayBits)
    {
      this._subArrayBits = subArrayBits;
    }
    
    public int getMaxEntrySize()
    {
      return this._maxEntrySize;
    }
    
    public void setMaxEntrySize(int maxEntrySize)
    {
      this._maxEntrySize = maxEntrySize;
    }

    public int getMaxEntries()
    {
      return this._maxEntries;
    }
    
    public void setMaxEntries(int maxEntries)
    {
      this._maxEntries = maxEntries;
    }
  }
}
