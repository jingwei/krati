package krati.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import krati.array.DataArray;
import krati.core.array.SimpleDataArray;
import krati.core.array.basic.DynamicLongArray;
import krati.core.segment.SegmentFactory;
import krati.core.segment.SegmentManager;
import krati.store.DataSet;
import krati.store.DataSetHandler;
import krati.util.FnvHashFunction;
import krati.util.HashFunction;

/**
 * DynamicDataSet is implemented using Linear Hashing. Its capacity grows as needed.
 * 
 * The values are stored in the underlying DataArray using the following format:
 * <pre>
 * [count:int][value-length:int][value:bytes][value-length:int][value:bytes]...
 *            +-----------value 1------------+-----------value 2-----------+
 * </pre>
 * 
 * @author jwu
 *
 */
public class DynamicDataSet implements DataSet<byte[]> {
    private final static Logger _log = Logger.getLogger(DynamicDataSet.class);
    
    private final double _loadThreshold;
    private final SimpleDataArray _dataArray;
    private final DynamicLongArray _addrArray;
    private final DataSetHandler _dataHandler;
    private final HashFunction<byte[]> _hashFunction;
    private volatile int _split;
    private volatile int _level;
    private volatile int _levelCapacity;
    private int _levelThreshold;
    private int _unitCapacity;
    private int _loadCount;
    
    /**
     * Creates a dynamic DataSet with the settings below:
     * 
     * <pre>
     *    Initial Level            : 0
     *    Entry Size               : 10000
     *    Max Entries              : 5
     *    Segment File Size        : 256MB
     *    Segment Compact Factor   : 0.5
     *    DataSet Hash Load Factor : 0.75
     *    DataSet Hash Function    : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir                the home directory of DataSet
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @throws Exception             if this dynamic data set cannot be created.
     */
    public DynamicDataSet(File homeDir, SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             0,     /* initial level */ 
             10000, /* entrySize */
             5,     /* maxEntries */
             256,   /* segmentFileSizeMB */
             segmentFactory,
             0.5,   /* segmentCompactFactor */
             0.75,  /* DataSet load factor */
             new FnvHashFunction());
    }
    
    /**
     * Creates a dynamic DataSet with the settings below:
     * 
     * <pre>
     *    Entry Size               : 10000
     *    Max Entries              : 5
     *    Segment File Size        : 256MB
     *    Segment Compact Factor   : 0.5
     *    DataSet Hash Load Factor : 0.75
     *    DataSet Hash Function    : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir                the home directory of DataSet
     * @param initLevel              the initial level when DataSet is created
     * @param segmentFactory         the segment factory
     * @throws Exception             if this dynamic data set cannot be created.
     */
    public DynamicDataSet(File homeDir,
                          int initLevel,
                          SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             initLevel,
             10000, /* entrySize */
             5,     /* maxEntries */
             256,   /* segmentFileSizeMB */
             segmentFactory,
             0.5,   /* segmentCompactFactor */
             0.75,  /* DataSet load factor */
             new FnvHashFunction());
    }
    
    /**
     * Creates a dynamic DataSet with the settings below:
     * 
     * <pre>
     *    Entry Size               : 10000
     *    Max Entries              : 5
     *    Segment File Size        : 256MB
     *    Segment Compact Factor   : 0.5
     *    DataSet Hash Load Factor : 0.75
     * </pre>
     * 
     * @param homeDir                the home directory of DataSet
     * @param initLevel              the initial level when DataSet is created
     * @param segmentFactory         the segment factory
     * @param hashFunction           the hash function for mapping values to indexes
     * @throws Exception             if this dynamic data set cannot be created.
     */
    public DynamicDataSet(File homeDir,
                          int initLevel,
                          SegmentFactory segmentFactory,
                          HashFunction<byte[]> hashFunction) throws Exception {
        this(homeDir,
             initLevel,
             10000, /* entrySize */
             5,     /* maxEntries */
             256,   /* segmentFileSizeMB */
             segmentFactory,
             0.5,   /* segmentCompactFactor */
             0.75,  /* DataSet load factor */
             hashFunction);
    }
    
    /**
     * Creates a dynamic DataSet with the settings below:
     * 
     * <pre>
     *    Entry Size               : 10000
     *    Max Entries              : 5
     *    Segment Compact Factor   : 0.5
     *    DataSet Hash Load Factor : 0.75
     *    DataSet Hash Function    : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir                the home directory of DataSet
     * @param initLevel              the initial level when DataSet is created
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @throws Exception             if this dynamic data set cannot be created.
     */
    public DynamicDataSet(File homeDir,
                          int initLevel,
                          int segmentFileSizeMB,
                          SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             initLevel,
             10000, /* entrySize */
             5,     /* maxEntries */
             segmentFileSizeMB,
             segmentFactory,
             0.5,   /* segmentCompactFactor */
             0.75,  /* DataSet load factor */
             new FnvHashFunction());
    }
    
    /**
     * Creates a dynamic DataSet with the settings below:
     * 
     * <pre>
     *    Entry Size               : 10000
     *    Max Entries              : 5
     *    Segment Compact Factor   : 0.5
     * </pre>
     * 
     * @param homeDir                the home directory of DataSet
     * @param initLevel              the initial level when DataSet is created
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @param hashLoadThreshold      the load factor of the underlying address array (hash table)
     * @param hashFunction           the hash function for mapping values to indexes
     * @throws Exception             if this dynamic data set cannot be created.
     */
    public DynamicDataSet(File homeDir,
                          int initLevel,
                          int segmentFileSizeMB,
                          SegmentFactory segmentFactory,
                          double hashLoadThreshold,
                          HashFunction<byte[]> hashFunction) throws Exception {
        this(homeDir,
             initLevel,
             10000, /* entrySize */
             5,     /* maxEntries */
             segmentFileSizeMB,
             segmentFactory,
             0.5,   /* segmentCompactFactor  */
             hashLoadThreshold,
             hashFunction);
    }
    
    /**
     * Creates a dynamic DataSet with the settings below:
     * 
     * <pre>
     *    Segment Compact Factor   : 0.5
     *    DataSet Hash Load Factor : 0.75
     *    DataSet Hash Function    : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir                the home directory of DataSet
     * @param initLevel              the initial level when DataSet is created
     * @param entrySize              the redo entry size (i.e., batch size)
     * @param maxEntries             the number of redo entries required for updating the underlying address array
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @throws Exception             if this dynamic data set cannot be created.
     */
    public DynamicDataSet(File homeDir,
                          int initLevel,
                          int entrySize,
                          int maxEntries,
                          int segmentFileSizeMB,
                          SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             initLevel,
             entrySize,
             maxEntries,
             segmentFileSizeMB,
             segmentFactory,
             0.5,   /* segmentCompactFactor */
             0.75,  /* DataSet load factor */
             new FnvHashFunction());
    }
    
    /**
     * Creates a dynamic DataSet.
     * 
     * <pre>
     *    Segment Compact Factor   : 0.5
     * </pre>
     * 
     * @param homeDir                the home directory of DataSet
     * @param initLevel              the initial level when DataSet is created
     * @param entrySize              the redo entry size (i.e., batch size)
     * @param maxEntries             the number of redo entries required for updating the underlying address array
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @param hashLoadThreshold      the load factor of the underlying address array (hash table)
     * @param hashFunction           the hash function for mapping values to indexes
     * @throws Exception             if this dynamic data set cannot be created.
     */
    public DynamicDataSet(File homeDir,
                          int initLevel,
                          int entrySize,
                          int maxEntries,
                          int segmentFileSizeMB,
                          SegmentFactory segmentFactory,
                          double hashLoadThreshold,
                          HashFunction<byte[]> hashFunction) throws Exception {
        this(homeDir,
             initLevel,
             entrySize,
             maxEntries,
             segmentFileSizeMB,
             segmentFactory,
             0.5,   /* segmentCompactFactor  */
             hashLoadThreshold,
             hashFunction);
    }
    
    /**
     * Creates a dynamic DataSet.
     * 
     * @param homeDir                the home directory of DataSet
     * @param initLevel              the initial level when DataSet is created
     * @param entrySize              the redo entry size (i.e., batch size)
     * @param maxEntries             the number of redo entries required for updating the underlying address array
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @param segmentCompactFactor   the load factor of segment, below which a segment is eligible for compaction
     * @param hashLoadThreshold      the load factor of the underlying address array (hash table)
     * @param hashFunction           the hash function for mapping values to indexes
     * @throws Exception             if this dynamic data set cannot be created.
     */
    public DynamicDataSet(File homeDir,
                          int initLevel,
                          int entrySize,
                          int maxEntries,
                          int segmentFileSizeMB,
                          SegmentFactory segmentFactory,
                          double segmentCompactFactor,
                          double hashLoadThreshold,
                          HashFunction<byte[]> hashFunction) throws Exception {
        _dataHandler = new DefaultDataSetHandler();
        
        // Create dynamic address array
        _addrArray = createAddressArray(entrySize, maxEntries, homeDir);
        _unitCapacity = _addrArray.subArrayLength();
        
        if(initLevel > 0) {
            _addrArray.expandCapacity(_unitCapacity * (1 << initLevel) - 1); 
        }
        
        // Create underlying segment manager
        String segmentHome = homeDir.getCanonicalPath() + File.separator + "segs";
        SegmentManager segmentManager = SegmentManager.getInstance(segmentHome, segmentFactory, segmentFileSizeMB);
        
        // Create underlying simple data array
        this._dataArray = new SimpleDataArray(_addrArray, segmentManager, segmentCompactFactor);
        this._hashFunction = hashFunction;
        this._loadThreshold = hashLoadThreshold;
        this._loadCount = scan();
        this.initLinearHashing();
        
        _log.info(getStatus());
    }
    
    protected DynamicLongArray createAddressArray(int entrySize,
                                                  int maxEntries,
                                                  File homeDirectory) throws Exception {
        return new DynamicLongArray(entrySize, maxEntries, homeDirectory);
    }
    
    protected long hash(byte[] value) {
        return _hashFunction.hash(value);
    }
    
    protected long nextScn() {
        return System.currentTimeMillis();
    }
    
    @Override
    public void sync() throws IOException {
        _dataArray.sync();
    }
    
    @Override
    public void persist() throws IOException {
        _dataArray.persist();
    }
    
    @Override
    public boolean has(byte[] value) {
        byte[] existingData;
        long hashCode = hash(value);
        
        /**
         * Need SPIN to retrieve data from the underlying array
         * because the index might have changed with the _split. 
         */
        
        // Map value to an array index
        int index = getIndex(hashCode);
        
        do {
            // Read existing data at the index
            existingData = _dataArray.get(index);
            
            // Check that value is still mapped to the known index
            int indexNew = getIndex(hashCode);
            if(index == indexNew) break;
            else index = indexNew;
        } while(true);
        
        return existingData == null ? false : _dataHandler.find(value, existingData);
    }
    
    public final int countCollisions(byte[] value) {
        byte[] existingData;
        long hashCode = hash(value);
        
        /**
         * Need SPIN to retrieve data from the underlying array
         * because the index might have changed with the _split. 
         */
        
        // Map value to an array index
        int index = getIndex(hashCode);
        
        do {
            // Read existing data at the index
            existingData = _dataArray.get(index);
            
            // Check that value is still mapped to the known index
            int indexNew = getIndex(hashCode);
            if(index == indexNew) break;
            else index = indexNew;
        } while(true);
        
        return existingData == null ? 0 : _dataHandler.countCollisions(value, existingData);
    }

    public final boolean hasWithoutCollisions(byte[] value) {
        return countCollisions(value) == 1;
    }
    
    @Override
    public synchronized boolean add(byte[] value) throws Exception {
        if(value == null) return false;
        
        if(0 < _split || _levelThreshold < _loadCount) {
            split();
        }
        
        int index = getIndex(value);
        return addInternal(index, value);
    }
    
    @Override
    public synchronized boolean delete(byte[] value) throws Exception {
        if(0 < _split || _levelThreshold < _loadCount) {
            split();
        }
        
        int index = getIndex(value);
        return deleteInternal(index, value);
    }
    
    @Override
    public synchronized void clear() throws IOException {
        _dataArray.clear();
        _loadCount = 0;
    }
    
    protected final int getIndex(byte[] value) {
        long hashCode = hash(value);
        int capacity = _levelCapacity;
        int index = (int)(hashCode % capacity);
        if (index < 0) index = -index;
        
        if (index < _split) {
            capacity = capacity << 1;
            index = (int)(hashCode % capacity);
            if (index < 0) index = -index;
        }
        
        return index;
    }
    
    protected final int getIndex(long hashCode) {
        int capacity = _levelCapacity;
        int index = (int)(hashCode % capacity);
        if (index < 0) index = -index;
        
        if (index < _split) {
            capacity = capacity << 1;
            index = (int)(hashCode % capacity);
            if (index < 0) index = -index;
        }
        
        return index;
    }
    
    protected boolean addInternal(int index, byte[] value) throws Exception {
        byte[] existingData = _dataArray.get(index);
        
        try {
            if(existingData == null || existingData.length == 0) {
                _dataArray.set(index, _dataHandler.assemble(value), nextScn());
                _loadCount++;
            } else {
                if(!_dataHandler.find(value, existingData)) {
                    _dataArray.set(index, _dataHandler.assemble(value, existingData), nextScn());
                }
            }
        } catch (Exception e) {
            _log.warn("Value reset at index="+ index + " value=\"" + new String(value) + "\"", e);
            _dataArray.set(index, _dataHandler.assemble(value), nextScn());
        }
        
        return true;
    }
    
    protected boolean deleteInternal(int index, byte[] value) throws Exception {
        try {
            byte[] existingData = _dataArray.get(index);
            if(existingData != null) {
               int newLength = _dataHandler.remove(value, existingData);
               if(newLength == 0) {
                   // entire data is removed
                   _dataArray.set(index, null, nextScn());
                   _loadCount--;
                   return true;
               } else if(newLength < existingData.length) {
                   // partial data is removed
                   _dataArray.set(index, existingData, 0, newLength, nextScn());
                   return true;
               }
               // else no data is removed
            }
        } catch (Exception e) {
            _log.warn("Failed to delete value=\""+ new String(value) + "\"", e);
            _dataArray.set(index, null, nextScn());
        }
        
        // no data is removed
        return false;
    }
    
    public final int getLevel() {
        return _level;
    }
    
    public final int getSplit() {
        return _split;
    }

    public final int getCapacity() {
        return _dataArray.length();
    }
    
    public final int getUnitCapacity() {
        return _unitCapacity;
    }
    
    public final int getLevelCapacity() {
        return _levelCapacity;
    }
    
    public final int getLoadCount() {
        return _loadCount;
    }
    
    public final double getLoadFactor() {
        return _loadCount / (double)getCapacity();
    }
    
    public final double getLoadThreshold() {
        return _loadThreshold;
    }
    
    private void initLinearHashing() throws Exception {
        int unitCount = _dataArray.length() / getUnitCapacity();
        
        if (unitCount == 1) {
            _level = 0;
            _split = 0;
            _levelCapacity = getUnitCapacity();
            _levelThreshold = (int)(_levelCapacity * _loadThreshold);
        } else {
            // Determine level and split
            _level = 0;
            int remainder = (unitCount - 1) >> 1;
            while(remainder > 0) {
                _level++;
                remainder = remainder >> 1;
            }
            
            _split = (unitCount - (1 << _level) - 1) * getUnitCapacity();
            _levelCapacity = getUnitCapacity() * (1 << _level);
            _levelThreshold = (int)(_levelCapacity * _loadThreshold);
            
            // Need to re-populate the last unit
            for(int i = 0, cnt = getUnitCapacity(); i < cnt; i++) {
                split();
            }
        }
    }
    
    private void split() throws Exception {
        // Ensure address capacity
        _addrArray.expandCapacity(_split + _levelCapacity);
        
        // Read data from the _split index
        byte[] data = _dataArray.get(_split);
        
        // Process read data
        if (data != null && data.length > 0) {
            ByteBuffer bb = ByteBuffer.wrap(data);
            int newCapacity = _levelCapacity << 1;
            
            int cnt = bb.getInt();
            while(cnt > 0) {
                // Read value
                int len = bb.getInt();
                byte[] value = new byte[len];
                bb.get(value);
                
                int newIndex = (int)(hash(value) % newCapacity);
                if (newIndex < 0) newIndex = -newIndex;
                
                if(newIndex != _split) {
                    // Remove at the old index
                    deleteInternal(_split, value);
                    
                    // Update at the new index
                    addInternal(newIndex, value);
                }
                
                cnt--;
            }
        }
        
        _split++;

        if(_split % _unitCapacity == 0) {
            _log.info("split " + getStatus());
        }
        
        if(_split == _levelCapacity) {
            _split = 0;
            _level++;
            _levelCapacity = getUnitCapacity() * (1 << _level);
            _levelThreshold = (int)(_levelCapacity * _loadThreshold);
            
            _log.info(getStatus());
        }
    }
    
    private int scan() {
        int cnt = 0;
        for(int i = 0, len = _dataArray.length(); i < len; i++) {
            if(_dataArray.hasData(i)) cnt++;
        }
        return cnt;
    }
    
    public synchronized void rehash() throws Exception {
        if(_split > 0) {
            do {
                split();
            } while(_split > 0);
            sync();
        } else if(getLoadFactor() > _loadThreshold) {
            do {
                split();
            } while(_split > 0);
            sync();
        }
    }
    
    /**
     * @return the status of this data set.
     */
    public String getStatus() {
        StringBuilder buf = new StringBuilder();
        
        buf.append("level=");
        buf.append(_level);
        buf.append(" split=");
        buf.append(_split);
        buf.append(" capacity=");
        buf.append(getCapacity());
        buf.append(" loadCount=");
        buf.append(_loadCount);
        buf.append(" loadFactor=");
        buf.append(getLoadFactor());
        
        return buf.toString();
    }
    
    /**
     * @return the underlying data array.
     */
    public DataArray getDataArray() {
        return _dataArray;
    }
}
