package krati.core.array.basic;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import krati.Persistable;
import krati.core.array.entry.Entry;
import krati.core.array.entry.EntryFactory;
import krati.core.array.entry.EntryPersistListener;
import krati.core.array.entry.EntryPool;
import krati.core.array.entry.EntryValue;
import krati.core.array.entry.PreFillEntryInt;
import krati.core.array.entry.PreFillEntryLong;
import krati.core.array.entry.PreFillEntryShort;

/**
 * ArrayEntryManager
 * 
 * @author jwu
 * 
 */
public class ArrayEntryManager<V extends EntryValue> implements Persistable {
  private static final Logger _log = Logger.getLogger(ArrayEntryManager.class);
  
  private final int _maxEntries;
  private final int _maxEntrySize;
  private volatile boolean _autoApplyEntries = true;   // Automatically apply accumulated entries to the array file
  private volatile long _lwmScn = 0;                   // Low water mark SCN starts from 0
  private volatile long _hwmScn = 0;                   // High water mark SCN starts from 0
  
  private RecoverableArray<V>  _array;
  private Entry<V>             _entry;           // current redo entry
  private Entry<V>             _entryCompaction; // current redo entry for compaction use
  private final EntryPool<V>   _entryPool;
  private final EntryApply<V>  _entryApply;
  private EntryPersistListener _persistListener;
  
  public ArrayEntryManager(RecoverableArray<V> array, int maxEntries, int maxEntrySize) {
    this._array = array;
    this._maxEntries = maxEntries;
    this._maxEntrySize = maxEntrySize;
    this._entryPool = new EntryPool<V>(array.getEntryFactory(), maxEntrySize);
    this._entryApply = new EntryApply<V>(this);
    this._entry = _entryPool.next();
    this._entryCompaction = _entryPool.next();
    
    _log.info("arrayLength=" + array.length() + " maxEntries=" + maxEntries + " maxEntrySize=" + maxEntrySize);
  }
  
  public int getMaxEntries() {
    return _maxEntries;
  }
  
  public int getMaxEntrySize() {
    return _maxEntrySize;
  }
  
  public File getDirectory() {
    return _array.getDirectory();
  }
  
  public EntryFactory<V> getEntryFactory() {
    return _array.getEntryFactory();
  }
  
  public boolean getAutoApplyEntries() {
    return _autoApplyEntries;
  }
  
  public void setAutoApplyEntries(boolean b) {
    _autoApplyEntries = b;
  }
  
  final void addToEntry(V entryValue) throws IOException {
      // Switch to a new entry if the current _entry has reached _maxEntrySize.
      if(_entry.isFull()) {
        switchEntry(false);
      }
      
      // Add to current entry
      _entry.add(entryValue);
      
      // Advance high water mark to maintain progress record
      _hwmScn = Math.max(_hwmScn, entryValue.scn);
      
      // Switch to a new entry if the current _entry has reached _maxEntrySize.
      if(_entry.isFull()) {
        switchEntry(false);
      }
  }
  
  final void addToEntryCompaction(V entryValue) throws IOException {
      // Switch to a new entry if the current _entryCompaction has reached _maxEntrySize.
      if(_entryCompaction.isFull()) {
        switchEntryCompaction(false);
      }
      
      // Add to current entry
      _entryCompaction.add(entryValue);
      
      // Switch to a new entry if the current _entryCompaction has reached _maxEntrySize.
      if(_entryCompaction.isFull()) {
        switchEntryCompaction(false);
      }
  }
  
  final void addToPreFillEntryInt(int pos, int val, long scn) throws IOException {
      // Switch to a new entry if the current _entry has reached _maxEntrySize.
      if(_entry.isFull()) {
        switchEntry(false);
      }
      
      // Add to current entry
      ((PreFillEntryInt)_entry).add(pos, val, scn);
      
      // Advance high water mark to maintain progress record
      _hwmScn = Math.max(_hwmScn, scn);
      
      // Switch to a new entry if the current _entry has reached _maxEntrySize.
      if(_entry.isFull()) {
        switchEntry(false);
      }
  }
  
  final void addToPreFillEntryLong(int pos, long val, long scn) throws IOException {
      // Switch to a new entry if the current _entry has reached _maxEntrySize.
      if(_entry.isFull()) {
        switchEntry(false);
      }
      
      // Add to current entry
      ((PreFillEntryLong)_entry).add(pos, val, scn);
      
      // Advance high water mark to maintain progress record
      _hwmScn = Math.max(_hwmScn, scn);
      
      // Switch to a new entry if the current _entry has reached _maxEntrySize.
      if(_entry.isFull()) {
        switchEntry(false);
      }
  }
  
  final void addToPreFillEntryLongCompaction(int pos, long val, long scn) throws IOException {
      // Switch to a new entry if the current _entryCompaction has reached _maxEntrySize.
      if(_entryCompaction.isFull()) {
        switchEntryCompaction(false);
      }
      
      // Add to current entry
      ((PreFillEntryLong)_entryCompaction).add(pos, val, scn);
      
      // Switch to a new entry if the current _entryCompaction has reached _maxEntrySize.
      if(_entryCompaction.isFull()) {
        switchEntryCompaction(false);
      }
  }
  
  final void addToPreFillEntryShort(int pos, short val, long scn) throws IOException {
      // Switch to a new entry if the current _entry has reached _maxEntrySize.
      if(_entry.isFull()) {
        switchEntry(false);
      }
      
      // Add to current entry
      ((PreFillEntryShort)_entry).add(pos, val, scn);
      
      // Advance high water mark to maintain progress record
      _hwmScn = Math.max(_hwmScn, scn);
      
      // Switch to a new entry if the current _entry has reached _maxEntrySize.
      if(_entry.isFull()) {
        switchEntry(false);
      }
  }
  
  public synchronized void clear() {
    _lwmScn = 0;
    _hwmScn = 0;
    _entry.clear();
    _entryCompaction.clear();
    _entryPool.clear();
    
    try {
        deleteEntryFiles();
    } catch(IOException e) {
        _log.warn(e.getMessage());
    }
    
    _log.info("entry files cleared");
  }
  
  @Override
  public long getHWMark() {
    return _hwmScn;
  }
  
  @Override
  public long getLWMark() {
    return _lwmScn;
  }
  
  @Override
  public void saveHWMark(long endOfPeriod) throws Exception {
    _hwmScn = Math.max(_hwmScn, endOfPeriod);
  }
  
  public void setWaterMarks(long lwmScn, long hwmScn) {
    if (lwmScn <= hwmScn) {
      _lwmScn = lwmScn;
      _hwmScn = hwmScn;
    }
  }
  
  @Override
  public void sync() throws IOException {
    /* ************************* *
     * Run in the blocking mode  *
     * ************************* */
    switchEntry(true);
    applyEntries(true);
  }
  
  @Override
  public void persist() throws IOException {
    /* ************************* *
     * Run in non-blocking mode  *
     * ************************* */
    switchEntry(false);
  }
  
  public void setEntryPersistListener(EntryPersistListener listener) {
    this._persistListener = listener;
  }
  
  public EntryPersistListener getEntryPersistListener() {
    return _persistListener;
  }
  
  /**
   * @return the name of entry log file.
   */
  protected final String getEntryLogName(Entry<V> entry) {
    return getEntryLogPrefix() + "_" + entry.getServiceId() + "_" + entry.getMinScn() + "_" + entry.getMaxScn() + getEntryLogSuffix();
  }
  
  /**
   * @return the prefix of entry log file.
   */
  protected final String getEntryLogPrefix() {
    return "entry";
  }
  
  /**
   * @return the suffix of entry log file.
   */
  protected final String getEntryLogSuffix() {
    return ".idx";
  }
  
  /**
   * Switches to a new entry if _curEntry is not empty.
   * 
   * @throws IOException
   */
  protected synchronized void switchEntry(boolean blocking) throws IOException {
    if (!_entry.isEmpty()) {
      if(_persistListener != null) {
        _persistListener.beforePersist(_entry);
      }
      
      // Create entry log and persist in-memory data
      File file = new File(getDirectory(), getEntryLogName(_entry));
      _entry.save(file);
      
      if(_persistListener != null) {
        _persistListener.afterPersist(_entry);
      }
      
      // Advance low water mark to maintain progress record
      _lwmScn = Math.max(_lwmScn, _entry.getMaxScn());
      _entryPool.addToServiceQueue(_entry);
      _entry = _entryPool.next();
      
      _log.info("switchEntry to " + _entry.getId() + " _lwmScn=" + _lwmScn + " _hwmScn=" + _hwmScn);
    }
    
    if (!_entryCompaction.isEmpty()) {
      // Create entry log and persist in-memory data
      File file = new File(getDirectory(), getEntryLogName(_entryCompaction));
      _entryCompaction.save(file);
      _entryPool.addToServiceQueue(_entryCompaction);
      _entryCompaction = _entryPool.next();
      
      _log.info("switchEntry to " + _entryCompaction.getId() + " _lwmScn=" + _lwmScn + " _hwmScn=" + _hwmScn + " Compaction");
    }
    
    // Apply entry logs to array file
    if (_autoApplyEntries) {
      if (_entryPool.getServiceQueueSize() >= _maxEntries) {
        applyEntries(blocking);
      }
    }
  }
  
  private synchronized void switchEntryCompaction(boolean blocking) throws IOException {
    if (!_entryCompaction.isEmpty()) {
      // Create entry log and persist in-memory data
      File file = new File(getDirectory(), getEntryLogName(_entryCompaction));
      _entryCompaction.save(file);
      _entryPool.addToServiceQueue(_entryCompaction);
      _entryCompaction = _entryPool.next();
      
      _log.info("switchEntry to " + _entryCompaction.getId() + " _lwmScn=" + _lwmScn + " _hwmScn=" + _hwmScn + " Compaction");
    }
    
    // Apply entry logs to array file
    if (_autoApplyEntries) {
      if (_entryPool.getServiceQueueSize() >= _maxEntries) {
        applyEntries(blocking);
      }
    }
  }
  
  /**
   * Apply accumulated entries to the array file.
   * @throws IOException
   */
  protected synchronized void applyEntries(boolean blocking) throws IOException {
    if (blocking) { /* Blocking Mode */
      synchronized(_entryApply) {
        List<Entry<V>> entryList = new ArrayList<Entry<V>>();
        while(_entryPool.getServiceQueueSize() > 0) {
          Entry<V> entry = _entryPool.pollFromService();
          if(entry != null) entryList.add(entry);
        }
        
        applyEntries(entryList);
      }
    } else {        /* Non-Blocking Mode */
      synchronized(_entryApply) {
        for(int i = 0; i < _maxEntries; i++) {
          Entry<V> entry = _entryPool.pollFromService();
          if(entry != null) _entryApply.add(entry);
        }
      }
      
      // Start a separate thread to update the underlying array file 
      new Thread(_entryApply).start();
    }
  }
  
  static class EntryApply<V extends EntryValue> implements Runnable {
    private final List<Entry<V>> _entryList;
    private final ArrayEntryManager<V> _entryManager;
    
    public EntryApply(ArrayEntryManager<V> entryManager) {
      _entryManager = entryManager;
      _entryList = new ArrayList<Entry<V>>();
    }
    
    public final void add(Entry<V> entry) {
      _entryList.add(entry);
    }
    
    @Override
    public final synchronized void run() {
      try {
        _entryManager.applyEntries(_entryList);
      } catch(IOException ioe) {
        _log.error(ioe.getMessage());
      }
    }
  }
  
  /**
   * Load entry log files from disk into _entryList.
   * 
   * @throws IOException
   */
  protected List<Entry<V>> loadEntryFiles() {
    File[] files = getDirectory().listFiles();
    String prefix = getEntryLogPrefix();
    String suffix = getEntryLogSuffix();
    
    List<Entry<V>> entryList = new ArrayList<Entry<V>>();
    
    for (File file : files) {
      String fileName = file.getName();
      if (fileName.startsWith(prefix) && fileName.endsWith(suffix)) {
        try {
          Entry<V> entry = _entryPool.next();
          entry.load(file);
          entryList.add(entry);
        } catch(IOException e) {
          String filePath = file.getAbsolutePath();
          _log.warn(filePath + " corrupted");
          if(file.delete()) {
            _log.warn(filePath + " deleted");
          }
        }
      }
    }
    
    return entryList;
  }
  
  /**
   * Delete entry log files on disk.
   * 
   * @throws IOException
   */
  protected void deleteEntryFiles() throws IOException {
    File[] files = getDirectory().listFiles();
    String prefix = getEntryLogPrefix();
    String suffix = getEntryLogSuffix();
    
    for (File file : files) {
      String fileName = file.getName();
      if (fileName.startsWith(prefix) && fileName.endsWith(suffix)) {
        if (file.delete()) {
          _log.info("file " + file.getAbsolutePath() + " deleted");
        } else {
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
  private void deleteEntryFiles(List<Entry<V>> list) throws IOException {
    for(Entry<V> e: list) {
      File file = e.getFile();
      if(file != null && file.exists()) {
        if (file.delete()) {
          _log.info(file.getName() + " deleted");
        } else {
          _log.warn(file.getName() + " not deleted");
        }
      }
    }
  }
  
  /**
   * Filter and select entries from an entry list
   * that only have SCNs no less than the specified lower bound minScn and
   * that only have SCNs no greater than the specified upper bound maxScn.
   * 
   * @param minScn Inclusive lower bound SCN.
   * @param maxScn Inclusive upper bound SCN.
   */
  private List<Entry<V>> filterEntryList(List<Entry<V>> entryList, long minScn, long maxScn) {
    List<Entry<V>> result = new ArrayList<Entry<V>>(entryList.size());
    for (Entry<V> e : entryList) {
      if (minScn <= e.getMinScn() && e.getMaxScn() <= maxScn) {
        result.add(e);
      }
    }
    
    return result;
  }
  
  /**
   * Filter and select entries from an entry list that only have SCNs no less than the specified lower bound.
   * 
   * @param scn Inclusive lower bound SCN
   */
  private List<Entry<V>> filterEntryListLowerBound(List<Entry<V>> entryList, long scn) {
    List<Entry<V>> result = new ArrayList<Entry<V>>(entryList.size());
    for (Entry<V> e : entryList) {
      if (scn <= e.getMinScn()) {
        result.add(e);
      }
    }
    
    return result;
  }
  
  /**
   * Filter and select entries from an entry list that only have SCNs no greater than the specified upper bound.
   * 
   * @param scn Inclusive upper bound SCN
   */
  @SuppressWarnings("unused")
  private List<Entry<V>> filterEntryListUpperBound(List<Entry<V>> entryList, long scn) {
    List<Entry<V>> result = new ArrayList<Entry<V>>(entryList.size());
    for (Entry<V> e : entryList) {
      if (e.getMaxScn() <= scn) {
        result.add(e);
      }
    }
    
    return result;
  }
  
  protected void applyEntries(List<Entry<V>> entries) throws IOException {
    // Update underlying array file
    _array.updateArrayFile(entries);
    
    // Clean up entry files
    deleteEntryFiles(entries);
    
    // Recycle all applied entries
    for(Entry<V> entry : entries) {
      _entryPool.addToRecycleQueue(entry);
    }
    
    entries.clear();
  }
  
  protected void init(long arrayFileLwmScn, long arrayFileHwmScn) throws IOException {
    // Load entries from logs on disk 
    List<Entry<V>> entryList = loadEntryFiles();
    
    // Sanitize loaded entries
    if (arrayFileLwmScn == arrayFileHwmScn) {
      // array file is consistent 
      // Find entries that have not been flushed to the array file.
      if(entryList.size() > 0) {
        entryList = filterEntryListLowerBound(entryList, arrayFileLwmScn);
      }
    } else {
      // array file is inconsistent
      if(entryList.size() > 0) {
        entryList = filterEntryList(entryList, arrayFileLwmScn, arrayFileHwmScn);
        
        if(entryList.size() == 0) {
          deleteEntryFiles();
          _log.error("entry files for recovery not found");
        }
      }
    }
    
    // Start recovery based on loaded entries.
    if (entryList.size() > 0) {
      applyEntries(entryList);
    }
    
    // Delete whatever entry files on disk.
    deleteEntryFiles();
  }
}
