package krati.cds.impl.array;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import krati.cds.array.DynamicArray;
import krati.cds.array.ShortArray;
import krati.cds.impl.array.basic.ShortArrayRecoverableImpl;

public class DynamicShortArrayImpl implements ShortArray, DynamicArray
{
  protected static final Logger log = Logger.getLogger(DynamicShortArrayImpl.class);

  protected long _lwmScn = 0;
  protected long _hwmScn = 0;
  protected short[][] _dataArrays = new short[0][0];
  protected ShortArrayRecoverableImpl[] _implArrays = new ShortArrayRecoverableImpl[0];
  
  protected final File _cacheDirectory;
  protected final int _maxEntrySize;
  protected final int _maxEntries;
  protected final int _subArrayShift;
  protected final int _subArraySize;
  protected final int _subArrayMask;
  
  public DynamicShortArrayImpl(Config config) throws Exception
  {
    this(config.getCacheDirectory(),
         config.getSubArrayShift(),
         config.getMaxEntrySize(),
         config.getMaxEntries());
  }
  
  public DynamicShortArrayImpl(File cacheDirectory,
                             int subArrayShift,
                             int maxEntrySize,
                             int maxEntries) throws Exception
  {
    this._cacheDirectory = cacheDirectory;
    this._subArrayShift = subArrayShift;
    this._maxEntrySize = maxEntrySize;
    this._maxEntries = maxEntries;
    this._subArraySize = 1 << subArrayShift;
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
    
    log.info("start to load cache ... _subArraySize="+_subArraySize);
    
    String prefix = "parallel_";
    String suffix = _subArraySize + ".dat";
    File[] files = _cacheDirectory.listFiles();
    
    // Find the maximum indexStart of all sub-arrays
    int indexStart = 0;
    boolean foundParallel = false;
    for (File file : files)
    {
      String fileName = file.getName();
      if (fileName.startsWith(prefix) && fileName.endsWith(suffix))
      {
        log.info("found parallel data " + fileName);
        
        int fromIndex = fileName.indexOf('_');
        int endIndex = fileName.indexOf('_', fromIndex + 1);
        if (fromIndex < endIndex)
        {
          String num = fileName.substring(fromIndex + 1, endIndex);
          try
          {
            indexStart = Math.max(indexStart, Integer.parseInt(num));
            log.info("parallel data index starts at " + indexStart);
            foundParallel = true;
          }
          catch(Exception e)
          {
            log.error(e.getMessage());
          }
        }
      }
    }
    
    if (foundParallel)
    {
      // Expand capacity to include the maximum indexStart of sub-arrays
      this.expandCapacity(indexStart);

      // Calculate _hwmScn by finding the smallest _hwmScn of all sub-arrays
      for(ShortArrayRecoverableImpl implArray : _implArrays)
      {
        long implHwmScn = implArray.getHWMark();
        if(implHwmScn > 0)
        {
          _hwmScn = (_hwmScn == 0) ? implHwmScn : Math.min(_hwmScn, implHwmScn);
        }
      }
      
      _lwmScn = getLWMark();
      log.info("cache loaded successfully: _lwmScn=" + _lwmScn + " _hwmScn=" + _hwmScn);
    }
  }
  
  /**
   * @return the current length of this ShortArray
   */
  public int length()
  {
    return _dataArrays.length * _subArraySize;
  }
  
  /**
   * @return the start index of this Array.
   */
  public final int getIndexStart()
  {
    return 0;
  }
  
  /**
   * @return a boolean indicating an index is in the current range of this ShortArray.
   */
  public boolean indexInRange(int index)
  {
    return (index >> _subArrayShift) < _dataArrays.length;
  }
  
  public short getData(int index)
  {
    int segInd = index >> _subArrayShift;
    int subInd = index & _subArrayMask;
    
    return _dataArrays[segInd][subInd];
  }
  
  public void setData(int index, short value, long scn) throws Exception
  {
    int segInd = index >> _subArrayShift;
    
    // Expand array capacity automatically
    if (segInd >= _implArrays.length)
    {
      try
      {
        expandCapacity(index);
      }
      catch(Exception e)
      {
        log.warn(e.getMessage());
      }
    }
    
    _implArrays[segInd].setData(index, value, scn);
    _hwmScn = Math.max(_hwmScn, scn);
  }
  
  public synchronized void expandCapacity(int index) throws Exception
  {
    int numSubArrays = (index >> _subArrayShift) + 1;
    if (numSubArrays <= _implArrays.length)
    {
      return;
    }
    
    ShortArrayRecoverableImpl[] tempArrays = new ShortArrayRecoverableImpl[numSubArrays];
    
    int i = 0;
    for (; i < _implArrays.length; i++)
    {
      tempArrays[i] = _implArrays[i];
    }
    
    for(; i < numSubArrays; i++)
    {
      int memberIdStart = i << _subArrayShift;
      int memberIdCount = _subArraySize;
      tempArrays[i] = 
        new ShortArrayRecoverableImpl(memberIdStart,
                                      memberIdCount,
                                      _maxEntrySize,
                                      _maxEntries,
                                      _cacheDirectory);
    }
    
    _implArrays = tempArrays;
    
    short[][] tempDataArrays = new short[tempArrays.length][];
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
    for(ShortArrayRecoverableImpl implArray : _implArrays)
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
    for(ShortArrayRecoverableImpl implArray : _implArrays)
    {
      implArray.saveHWMark(mark);
    }
    _hwmScn = mark;
  }
  
  public void persist() throws IOException
  {
    // Sync high-water marks
    saveHWMark(getHWMark());
      
    // Persist each sub-array
    for(ShortArrayRecoverableImpl implArray : _implArrays)
    {
      try
      {
        implArray.persist();
      }
      catch(IOException e)
      {
        log.error(e.getMessage());
      }
    }
  }
  
  public void clear()
  {
    for(ShortArrayRecoverableImpl implArray : _implArrays)
    {
      implArray.clear();
    }
  }
  
  public short[] getInternalArray()
  {
    short[] result = new short[length()];
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
    private int _subArrayShift;
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
    
    public int getSubArrayShift()
    {
      return this._subArrayShift;
    }
    
    public void setSubArrayShift(int subArrayShift)
    {
      this._subArrayShift = subArrayShift;
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
