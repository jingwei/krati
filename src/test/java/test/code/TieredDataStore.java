package test.code;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import krati.array.DataArray;
import krati.core.array.SimpleDataArray;
import krati.core.array.basic.DynamicLongArray;
import krati.core.segment.SegmentFactory;
import krati.core.segment.SegmentManager;
import krati.store.DataStore;
import krati.store.DataStoreHandler;
import krati.store.DefaultDataStoreHandler;
import krati.util.FnvHashFunction;
import krati.util.HashFunction;

/**
 * TieredDataStore is implemented using Linear Hashing. Its capacity grows as needed.
 * 
 * The key-value pairs are stored in the underlying DataArray using the following format:
 * <pre>
 * [count:int][key-length:int][key:bytes][value-length:int][value:bytes][key-length:int][key:bytes][value-length:int][value:bytes]...
 *            +------------------ key-value pair 1 ---------------------+------------------- key-value pair 2 -------------------+
 * </pre>
 * 
 * @author jwu
 *
 */
public class TieredDataStore implements DataStore<byte[], byte[]> {
    private final static Logger _log = Logger.getLogger(TieredDataStore.class);
    
    private final double _loadThreshold;
    private final SimpleDataArray _dataArray;
    private final DynamicLongArray _addrArray;
    private final DataStoreHandler _dataHandler;
    private final HashFunction<byte[]> _hashFunction;
    protected volatile int _split;
    protected volatile int _level;
    protected volatile int _levelCapacity;
    private int _levelThreshold;
    private int _unitCapacity;
    private int _loadCount;
    
    /**
     * Creates a dynamic DataStore with the settings below:
     * 
     * <pre>
     *    Initial Level            : 0
     *    Entry Size               : 10000
     *    Max Entries              : 5
     *    Segment File Size        : 256MB
     *    Segment Compact Factor   : 0.5
     *    Store Hash Load Factor   : 0.75
     *    Store Hash Function      : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir                the home directory of DataStore
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @throws Exception             if this dynamic data store cannot be created.
     */
    public TieredDataStore(File homeDir, SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             0,     /* initial level */ 
             10000, /* entrySize */
             5,     /* maxEntries */
             256,   /* segmentFileSizeMB */
             segmentFactory,
             0.5,   /* segmentCompactFactor  */
             0.75,  /* DataStore load factor */
             new FnvHashFunction());
    }
    
    /**
     * Creates a dynamic DataStore with the settings below:
     * 
     * <pre>
     *    Entry Size               : 10000
     *    Max Entries              : 5
     *    Segment File Size        : 256MB
     *    Segment Compact Factor   : 0.5
     *    Store Hash Load Factor   : 0.75
     *    Store Hash Function      : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir                the home directory of DataStore
     * @param initLevel              the initial level when DataStore is created
     * @param segmentFactory         the segment factory
     * @throws Exception             if this dynamic data store cannot be created.
     */
    public TieredDataStore(File homeDir,
                           int initLevel,
                           SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             initLevel,
             10000, /* entrySize */
             5,     /* maxEntries */
             256,   /* segmentFileSizeMB */
             segmentFactory,
             0.5,   /* segmentCompactFactor  */
             0.75,  /* DataStore load factor */
             new FnvHashFunction());
    }
    
    /**
     * Creates a dynamic DataStore with the settings below:
     * 
     * <pre>
     *    Entry Size               : 10000
     *    Max Entries              : 5
     *    Segment File Size        : 256MB
     *    Segment Compact Factor   : 0.5
     *    Store Hash Load Factor   : 0.75
     * </pre>
     * 
     * @param homeDir                the home directory of DataStore
     * @param initLevel              the initial level when DataStore is created
     * @param segmentFactory         the segment factory
     * @param hashFunction           the hash function for mapping keys to indexes
     * @throws Exception             if this dynamic data store cannot be created.
     */
    public TieredDataStore(File homeDir,
                           int initLevel,
                           SegmentFactory segmentFactory,
                           HashFunction<byte[]> hashFunction) throws Exception {
        this(homeDir,
             initLevel,
             10000, /* entrySize */
             5,     /* maxEntries */
             256,   /* segmentFileSizeMB */
             segmentFactory,
             0.5,   /* segmentCompactFactor  */
             0.75,  /* DataStore load factor */
             hashFunction);
    }
    
    /**
     * Creates a dynamic DataStore with the settings below:
     * 
     * <pre>
     *    Entry Size               : 10000
     *    Max Entries              : 5
     *    Segment Compact Factor   : 0.5
     *    Store Hash Load Factor   : 0.75
     *    Store Hash Function      : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir                the home directory of DataStore
     * @param initLevel              the initial level when DataStore is created
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @throws Exception             if this dynamic data store cannot be created.
     */
    public TieredDataStore(File homeDir,
                           int initLevel,
                           int segmentFileSizeMB,
                           SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             initLevel,
             10000, /* entrySize */
             5,     /* maxEntries */
             segmentFileSizeMB,
             segmentFactory,
             0.5,   /* segmentCompactFactor  */
             0.75,  /* DataStore load factor */
             new FnvHashFunction());
    }
    
    /**
     * Creates a dynamic DataStore with the settings below:
     * 
     * <pre>
     *    Entry Size               : 10000
     *    Max Entries              : 5
     *    Segment Compact Factor   : 0.5
     * </pre>
     * 
     * @param homeDir                the home directory of DataStore
     * @param initLevel              the initial level when DataStore is created
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @param hashLoadThreshold      the load factor of the underlying address array (hash table)
     * @param hashFunction           the hash function for mapping keys to indexes
     * @throws Exception             if this dynamic data store cannot be created.
     */
    public TieredDataStore(File homeDir,
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
     * Creates a dynamic DataStore with the settings below:
     * 
     * <pre>
     *    Segment Compact Factor   : 0.5
     *    Store Hash Load Factor   : 0.75
     *    Store Hash Function      : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir                the home directory of DataStore
     * @param initLevel              the initial level when DataStore is created
     * @param entrySize              the redo entry size (i.e., batch size)
     * @param maxEntries             the number of redo entries required for updating the underlying address array
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @throws Exception             if this dynamic data store cannot be created.
     */
    public TieredDataStore(File homeDir,
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
             0.5,   /* segmentCompactFactor  */
             0.75,  /* DataStore load factor */
             new FnvHashFunction());
    }
    
    /**
     * Creates a dynamic DataStore.
     * 
     * <pre>
     *    Segment Compact Factor   : 0.5
     * </pre>
     * 
     * @param homeDir                the home directory of DataStore
     * @param initLevel              the initial level when DataStore is created
     * @param entrySize              the redo entry size (i.e., batch size)
     * @param maxEntries             the number of redo entries required for updating the underlying address array
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @param hashLoadThreshold      the load factor of the underlying address array (hash table)
     * @param hashFunction           the hash function for mapping keys to indexes
     * @throws Exception             if this dynamic data store cannot be created.
     */
    public TieredDataStore(File homeDir,
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
     * Creates a dynamic DataStore.
     * 
     * @param homeDir                the home directory of DataStore
     * @param initLevel              the initial level when DataStore is created
     * @param entrySize              the redo entry size (i.e., batch size)
     * @param maxEntries             the number of redo entries required for updating the underlying address array
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @param segmentCompactFactor   the load factor of segment, below which a segment is eligible for compaction
     * @param hashLoadThreshold      the load factor of the underlying address array (hash table)
     * @param hashFunction           the hash function for mapping keys to indexes
     * @throws Exception             if this dynamic data store cannot be created.
     */
    public TieredDataStore(File homeDir,
                           int initLevel,
                           int entrySize,
                           int maxEntries,
                           int segmentFileSizeMB,
                           SegmentFactory segmentFactory,
                           double segmentCompactFactor,
                           double hashLoadThreshold,
                           HashFunction<byte[]> hashFunction) throws Exception {
        // Create data store handler
        _dataHandler = new DefaultDataStoreHandler();
        
        // Create dynamic address array
        _addrArray = createAddressArray(entrySize, maxEntries, homeDir);
        _unitCapacity = _addrArray.subArrayLength();
        
        if (initLevel > 0) {
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
        
        // Create three tiers
        _tier1 = createTier(1);
        _tier2 = createTier(2);
        _tier3 = createTier(3);
        
        _log.info(getStatus());
    }
    
    protected DynamicLongArray createAddressArray(int entrySize,
                                                  int maxEntries,
                                                  File homeDirectory) throws Exception {
        return new DynamicLongArray(entrySize, maxEntries, homeDirectory);
    }
    
    protected long hash(byte[] key) {
        return _hashFunction.hash(key);
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
    public byte[] get(byte[] key) {
        byte[] value;
        byte[] tierData;
        long hashCode = hash(key);
        
        tierData = getInternal(_tier1, hashCode);
        value = (tierData == null) ? null : _dataHandler.extractByKey(key, tierData);
        
        if(value == null) {
            tierData = getInternal(_tier2, hashCode);
            value = (tierData == null) ? null : _dataHandler.extractByKey(key, tierData);
            
            if(value == null) {
                tierData = getInternal(_tier3, hashCode);
                value = (tierData == null) ? null : _dataHandler.extractByKey(key, tierData);
            }
        }
        
        return value;
    }
    
    public synchronized boolean put(byte[] key, byte[] value) throws Exception {
        if(value == null) {
            return delete(key);
        }
        
        if(0 < _split || _levelThreshold < _loadCount) {
            split();
        }
        
        int index;
        long hashCode = hash(key);
        
        index = _tier1.getMainIndex(hashCode, _level, _split);
        if(putInternalNonColliding(index, key, value)) {
            index = _tier2.getMainIndex(hashCode, _level, _split);
            deleteInternal(index, key);
            
            index = _tier3.getMainIndex(hashCode, _level, _split);
            deleteInternal(index, key);
            
            return true;
        }
        
        index = _tier2.getMainIndex(hashCode, _level, _split);
        if(putInternalNonColliding(index, key, value)) {
            index = _tier3.getMainIndex(hashCode, _level, _split);
            deleteInternal(index, key);
            
            return true;
        }
        
        index = _tier3.getMainIndex(hashCode, _level, _split);
        return putInternal(index, key, value);
    }
    
    @Override
    public synchronized boolean delete(byte[] key) throws Exception {
        if(0 < _split || _levelThreshold < _loadCount) {
            split();
        }
        
        int index;
        long hashCode = hash(key);
        
        index = _tier1.getMainIndex(hashCode, _level, _split);
        if(deleteInternal(index, key)) {
            return true;
        }
        
        index = _tier2.getMainIndex(hashCode, _level, _split);
        if(deleteInternal(index, key)) {
            return true;
        }
        
        index = _tier3.getMainIndex(hashCode, _level, _split);
        return deleteInternal(index, key);
    }
    
    @Override
    public synchronized void clear() throws IOException {
        _dataArray.clear();
        _loadCount = 0;
    }
    
    private byte[] getInternal(Tier tier, long hashCode) {
        byte[] existingData ;
        
        /**
         * Need SPIN to retrieve data from the underlying array
         * because the index might have changed with the _split. 
         */
        
        // Map key to an array index
        int index = tier.getMainIndex(hashCode, _level, _split);
        
        do {
            // Read existing data at the index
            existingData = _dataArray.get(index);
            
            // Check that key is still mapped to the known index
            int indexNew = tier.getMainIndex(hashCode, _level, _split);
            if (indexNew == index) break;
            index = indexNew;
        } while(true);
        
        return existingData;
    }
    
    private boolean putInternalNonColliding(int index, byte[] key, byte[] value) throws Exception {
        byte[] existingData = _dataArray.get(index);
        
        if(existingData == null) {
            return setInternal(index, key, value);
        }
        
        int collisionCnt = _dataHandler.countCollisions(key, existingData);
        if(collisionCnt == 0) {
            return setInternal(index, key, value);
        } else if(collisionCnt == 1) {
            return putReplace(index, key, value);
        } else {
            return false;
        }
    }
    
    private boolean putInternal(int index, byte[] key, byte[] value) throws Exception {
        byte[] existingData = _dataArray.get(index);
        
        if(existingData == null || existingData.length == 0) {
            _dataArray.set(index, _dataHandler.assemble(key, value), nextScn());
            _loadCount++;
        } else {
            try {
                _dataArray.set(index, _dataHandler.assemble(key, value, existingData), nextScn());
            } catch(Exception e) {
                _log.warn("Value reset at index="+ index + " key=\"" + new String(key) + "\"");
                _dataArray.set(index, _dataHandler.assemble(key, value), nextScn());
            }
        }
        
        return true;
    }
    
    private boolean setInternal(int index, byte[] key, byte[] value) throws Exception {
        _dataArray.set(index, _dataHandler.assemble(key, value), nextScn());
        _loadCount++;
        return true;
    }
    
    private boolean putReplace(int index, byte[] key, byte[] value) throws Exception {
        _dataArray.set(index, _dataHandler.assemble(key, value), nextScn());
        return true;
    }
    
    private boolean deleteInternal(int index, byte[] key) throws Exception {
        try {
            byte[] existingData = _dataArray.get(index);
            if (existingData != null) {
                int newLength = _dataHandler.removeByKey(key, existingData);
                if (newLength == 0) {
                    // entire data is removed
                    _dataArray.set(index, null, nextScn());
                    _loadCount--;
                    return true;
                } else if (newLength < existingData.length) {
                    // partial data is removed
                    _dataArray.set(index, existingData, 0, newLength, nextScn());
                    return true;
                }
            }
        } catch (Exception e) {
            _log.warn("Failed to delete key=\"" + new String(key) + "\" : " + e.getMessage());
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
            while (remainder > 0) {
                _level++;
                remainder = remainder >> 1;
            }

            _split = (unitCount - (1 << _level) - 1) * getUnitCapacity();
            _levelCapacity = getUnitCapacity() * (1 << _level);
            _levelThreshold = (int) (_levelCapacity * _loadThreshold);

            // Need to re-populate the last unit
            for (int i = 0, cnt = getUnitCapacity(); i < cnt; i++) {
                split();
            }
        }
    }
    
    protected synchronized void split() throws Exception {
        // Ensure address capacity
        _addrArray.expandCapacity(_split + _levelCapacity);
        
        // Read data from the _split index
        byte[] data = _dataArray.get(_split);
        
        // Process read data
        if (data != null && data.length > 0) {
            // Get split tier
            Tier tier = getTier(_split);
            
            // Wrap data in byte buffer
            ByteBuffer bb = ByteBuffer.wrap(data);
            
            int cnt = bb.getInt();
            
            if (tier.isColliding()) {
                while (cnt > 0) {
                    // Read key
                    int len = bb.getInt();
                    byte[] key = new byte[len];
                    bb.get(key);
                    
                    int newIndex = tier.getSplitIndex(hash(key), _level, _split);
                    if (newIndex == _split) { /* No need to split */
                        // Pass value
                        len = bb.getInt();
                        bb.position(bb.position() + len);
                    } else {
                        // Read value
                        len = bb.getInt();
                        byte[] value = new byte[len];
                        bb.get(value);

                        // Remove at the old index
                        deleteInternal(_split, key);

                        // Update at the new index
                        putInternal(newIndex, key, value);
                    }
                    
                    cnt--;
                }
            } else {
                // Read key
                int len = bb.getInt();
                byte[] key = new byte[len];
                bb.get(key);
                
                int newIndex = tier.getSplitIndex(hash(key), _level, _split);
                if(newIndex != _split) {
                    long scn = nextScn();
                    _addrArray.set(newIndex, _addrArray.get(_split), scn);
                    _addrArray.set(_split, 0, scn);
                }
            }
        }
        
        _split++;
        
        if (_split % _unitCapacity == 0) {
            _log.info("split " + getStatus());
        }

        if (_split == _levelCapacity) {
            _split = 0;
            _level++;
            _levelCapacity = getUnitCapacity() * (1 << _level);
            _levelThreshold = (int) (_levelCapacity * _loadThreshold);

            _log.info(getStatus());
        }
    }
    
    private int scan() {
        int cnt = 0;
        for (int i = 0, len = _dataArray.length(); i < len; i++) {
            if(_dataArray.hasData(i)) cnt++;
        }
        return cnt;
    }
    
    public synchronized void rehash() throws Exception {
        if (_split > 0) {
            do {
                split();
            } while (_split > 0);
            sync();
        } else if (getLoadFactor() > _loadThreshold) {
            do {
                split();
            } while (_split > 0);
            sync();
        }
    }
    
    /**
     * @return the status of this data store.
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
    
    private final Tier _tier1;
    private final Tier _tier2;
    private final Tier _tier3;
    
    protected Tier createTier(int rank) {
        int k = 1 << 10;
        switch(rank) {
        case 1 : return new Tier(false, 36 * k, getUnitCapacity(), 0);
        case 2 : return new Tier(false, 18 * k, getUnitCapacity(), 36 * k);
        default: return new Tier(true , 10 * k, getUnitCapacity(), 54 * k);
        }
    }
    
    protected final Tier getTier1() {
        return _tier1;
    }
    
    protected final Tier getTier2() {
        return _tier2;
    }
    
    protected final Tier getTier3() {
        return _tier3;
    }
    
    protected Tier getTier(int index) {
        return _tier1.hasIndex(index) ? _tier1 : (_tier2.hasIndex(index) ? _tier2 : _tier3);
    }
    
    public static class Tier {
        private final boolean _colliding;
        private final int _tierUnitCapacity;
        private final int _mainUnitCapacity;
        private final int _mainUnitStart;
        private final int _mainUnitEnd;
        
        public Tier(boolean colliding, int tierUnitCapacity, int mainUnitCapacity, int mainUnitOffset) {
            this._colliding = colliding;
            this._tierUnitCapacity = tierUnitCapacity;
            this._mainUnitCapacity = mainUnitCapacity;
            this._mainUnitStart = mainUnitOffset;
            this._mainUnitEnd = mainUnitOffset + tierUnitCapacity;
        }
        
        public boolean isColliding() {
            return _colliding;
        }
        
        public int getMainIndex(long hashCode, int mainLevel, int mainSplit) {
            int tierLevelCapacity = _tierUnitCapacity * (1 << mainLevel);
            int tierIndex = (int)(hashCode % tierLevelCapacity);
            if (tierIndex < 0) tierIndex = -tierIndex;
            
            int unitCount = tierIndex / _tierUnitCapacity;
            int remainder = tierIndex % _tierUnitCapacity;
            int mainIndex = (unitCount * _mainUnitCapacity) + _mainUnitStart + remainder;
            
            if (mainIndex < mainSplit) {
                tierLevelCapacity = tierLevelCapacity << 1;
                tierIndex = (int)(hashCode % tierLevelCapacity);
                if (tierIndex < 0) tierIndex = -tierIndex;
                
                unitCount = tierIndex / _tierUnitCapacity;
                remainder = tierIndex % _tierUnitCapacity;
                mainIndex = (unitCount * _mainUnitCapacity) + _mainUnitStart + remainder;
            }
            
            return mainIndex;
        }
        
        protected int getSplitIndex(long hashCode, int mainLevel, int mainSplit) {
            int tierLevelCapacity = _tierUnitCapacity * (1 << (mainLevel + 1));
            int tierIndex = (int)(hashCode % tierLevelCapacity);
            if (tierIndex < 0) tierIndex = -tierIndex;
            
            int unitCount = tierIndex / _tierUnitCapacity;
            int remainder = tierIndex % _tierUnitCapacity;
            int mainIndex = (unitCount * _mainUnitCapacity) + _mainUnitStart + remainder;
            
            return mainIndex;
        }
        
        protected boolean hasIndex(int mainIndex) {
            int remainder = mainIndex % _mainUnitCapacity;
            return (_mainUnitStart <= remainder && remainder < _mainUnitEnd) ? true : false;
        }
    }

    @Override
    public Iterator<byte[]> keyIterator() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Entry<byte[], byte[]>> iterator() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }
}
