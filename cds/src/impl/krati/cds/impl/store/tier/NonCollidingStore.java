package krati.cds.impl.store.tier;

import java.io.File;
import java.io.IOException;

import krati.cds.array.DataArray;
import krati.cds.impl.array.SimpleDataArray;
import krati.cds.impl.array.basic.DynamicLongArrayDual;
import krati.cds.impl.segment.SegmentFactory;
import krati.cds.impl.segment.SegmentManager;
import krati.cds.impl.store.DefaultStoreDataHandler;
import krati.cds.store.DataStore;
import krati.cds.store.StoreDataHandler;
import krati.util.HashFunction;

import org.apache.log4j.Logger;

public class NonCollidingStore implements DataStore<byte[], byte[]>
{
    final static Logger _log = Logger.getLogger(NonCollidingStore.class);
    
    private final SimpleDataArray      _dataArray;
    private final DynamicLongArrayDual _addrArray;
    private final StoreDataHandler     _dataHandler;
    private final HashFunction<byte[]> _hashFunction;
    private volatile int _split;
    private volatile int _level;
    private volatile int _levelCapacity;
    private int _unitCapacity;
    private int _loadCount;
    
    /**
     * Creates a dynamic DataStore.
     * 
     * @param homeDir                the home directory of DataStore
     * @param initLevel              the initial level when DataStore is created
     * @param entrySize              the redo entry size (i.e., batch size)
     * @param maxEntries             the number of redo entries required for updating the underlying address array
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @param segmentCompactTrigger  the percentage of segment capacity, which triggers compaction once per segment
     * @param segmentCompactFactor   the load factor of segment, below which a segment is eligible for compaction
     * @param hashLoadFactor         the load factor of the underlying address array (hash table)
     * @param hashFunction           the hash function for mapping keys to indexes
     * @throws Exception             if this dynamic data store cannot be created.
     */
    public NonCollidingStore(File homeDir,
                            int initLevel,
                            int entrySize,
                            int maxEntries,
                            int segmentFileSizeMB,
                            SegmentFactory segmentFactory,
                            double segmentCompactTrigger,
                            double segmentCompactFactor,
                            HashFunction<byte[]> hashFunction) throws Exception
    {
        // Create store data handler
        _dataHandler = new DefaultStoreDataHandler();
        
        // Create dynamic address array
        _addrArray = createIndexes(entrySize, maxEntries, homeDir);
        _unitCapacity = _addrArray.subArrayLength();
        
        if(initLevel > 0)
        {
            _addrArray.expandCapacity(_unitCapacity * (1 << initLevel) - 1);
        }
        
        // Create underlying segment manager
        String segmentHome = homeDir.getCanonicalPath() + File.separator + "segs";
        SegmentManager segmentManager = SegmentManager.getInstance(segmentHome, segmentFactory, segmentFileSizeMB);
        
        // Create underlying simple data array
        this._dataArray = new SimpleDataArray(_addrArray, segmentManager, segmentCompactTrigger, segmentCompactFactor);
        this._hashFunction = hashFunction;
        this._loadCount = scan();
        this.initLinearHashing();
        
        _log.info(getStatus());
    }
    
    protected DynamicLongArrayDual createIndexes(int entrySize,
                                                 int maxEntries,
                                                 File homeDirectory) throws Exception
    {
        return new DynamicLongArrayDual(entrySize, maxEntries, homeDirectory, "indexes.dat", "indexes.hash.dat");
    }
    
    protected long hash(byte[] key)
    {
        return _hashFunction.hash(key);
    }
    
    @Override
    public synchronized void sync() throws IOException
    {
        _dataArray.sync();
    }
    
    @Override
    public synchronized void persist() throws IOException
    {
        _dataArray.persist();
    }
    
    /**
     * Assumes that key is not null.
     */
    @Override
    public byte[] get(byte[] key)
    {
        byte[] existingData = getExistingData(key);
        return existingData == null ? null : _dataHandler.extractByKey(key, existingData);
    }
    
    /**
     * Assumes that key and value are not null.
     */
    @Override
    public synchronized boolean put(byte[] key, byte[] value) throws Exception
    {
        long hashCode = hash(key);
        return put(hashCode, key, value);
    }
    
    /**
     * Assumes that key is not null.
     */
    @Override
    public synchronized boolean delete(byte[] key) throws Exception
    {
        long hashCode = hash(key);
        return delete(hashCode, key);
    }
    
    /**
     * Puts a given key-value pair into this store.
     * Assumes that key and value are not null.
     * 
     * @param hashCode  the hash code of key
     * @param key       the key
     * @param value     the value
     * @return <code>true</code> if the key-value pair can be put into this store without collisions.
     * @throws Exception
     */
    protected synchronized boolean put(long hashCode, byte[] key, byte[] value) throws Exception
    {
        // Split if the split cycle has been started by the colliding tier
        if(_split > 0) trySplit();
        
        // Get the index based on the given key hash code
        int index = getIndex(hashCode);
        
        long existingHashCode = _addrArray.getDual(index);
        if(existingHashCode != HashFunction.NON_HASH_CODE)
        {
            if(hashCode != existingHashCode)
            {
                /**
                 * Collision found via hash code
                 */
                return false;
            }
            else
            {
                /**
                 * Check collision based on real key data
                 */
                byte[] existingData = _dataArray.getData(index);
                if(existingData != null)
                {
                    int collisionCnt = _dataHandler.countCollisions(key, existingData);
                    if(collisionCnt == 0)
                    {
                        return putInternal(index, hashCode, key, value);
                    }
                    else if(collisionCnt == 1)
                    {
                        return putReplace(index, key, value);
                    }
                    else
                    {
                        return false;
                    }
                }
            }
        }
        
        // OK to put this key-value pair
        return putInternal(index, hashCode, key, value);
    }
    
    /**
     * Delete a key-value pair from this store based on a given key.
     * Assumes that key is not null.
     * 
     * @param hashCode  the hash code of key
     * @param key       the key
     * @return <code>true</code> if the delete is successful.
     * @throws Exception
     */
    protected synchronized boolean delete(long hashCode, byte[] key) throws Exception
    {
        // Split if the split cycle has been started by the colliding tier
        if(_split > 0) trySplit();
        
        // Get the index based on the given key hash code
        int index = getIndex(hashCode);
        
        long existingHashCode = _addrArray.getDual(index);
        if(existingHashCode == HashFunction.NON_HASH_CODE) return false;
        
        if(hashCode != existingHashCode)
        {
            /**
             * Collision found via hash code 
             */
            return false;
        }
        else
        {
            /**
             * Check collision based on real key data
             */
            byte[] existingData = _dataArray.getData(index);
            if(existingData != null)
            {
                int collisionCnt = _dataHandler.countCollisions(key, existingData);
                if(collisionCnt != 0 && collisionCnt != 1) return false;
            }
        }
        
        // OK to delete this key 
        return deleteInternal(index, key);
    }
    
    @Override
    public synchronized void clear() throws IOException
    {
        _dataArray.clear();
        _loadCount = 0;
    }
    
    protected final int getIndex(byte[] key)
    {
        long hashCode = hash(key);
        return getIndex(hashCode);
    }
    
    protected final int getIndex(long hashCode)
    {
        int capacity = _levelCapacity;
        int index = (int)(hashCode % capacity);
        if (index < 0) index = -index;
        
        if (index < _split)
        {
            capacity = capacity << 1;
            index = (int)(hashCode % capacity);
            if (index < 0) index = -index;
        }
        
        return index;
    }
    
    protected long getExistingHashCode(long hashCode)
    {
        long existingHashCode = HashFunction.NON_HASH_CODE;
        
        /**
         * Need SPIN to retrieve data from the underlying array
         * because the index might have changed with the _split. 
         */
        
        // Map key to an array index
        int index = getIndex(hashCode);
        
        do
        {
            // Get the existing hashCode
            existingHashCode = _addrArray.getDual(index);
            
            // Check that key is still mapped to the known index
            int indexNew = getIndex(hashCode);
            if(index == indexNew) break;
            else index = indexNew;
        } while(true);
        
        return existingHashCode;
    }

    protected byte[] getExistingData(byte[] key)
    {
        long hashCode = hash(key);
        return getExistingData(hashCode);
    }
    
    protected byte[] getExistingData(long hashCode)
    {
        byte[] existingData;
        
        // Map key to an array index
        int index = getIndex(hashCode);
        
        // Compare to the existing hashCode
        if(hashCode != getExistingHashCode(hashCode)) return null;
        
        /**
         * Need SPIN to retrieve data from the underlying array
         * because the index might have changed with the _split. 
         */
        do
        {
            // Read existing data at the index
            existingData = _dataArray.getData(index);
            
            // Check that key is still mapped to the known index
            int indexNew = getIndex(hashCode);
            if(index == indexNew) break;
            else index = indexNew;
        } while(true);
        
        return existingData;
    }
    
    protected boolean putInternal(int index, long hashCode, byte[] key, byte[] value) throws Exception
    {
        long scn = nextScn();
        _dataArray.setData(index, _dataHandler.assemble(key, value), scn);
        _addrArray.setDual(index, hashCode, scn);
        _loadCount++;
        return true;
    }
    
    protected boolean putReplace(int index, byte[] key, byte[] value) throws Exception
    {
        _dataArray.setData(index, _dataHandler.assemble(key, value), nextScn());
        return true;
    }
    
    protected boolean deleteInternal(int index, byte[] key) throws Exception
    {
        long scn = nextScn();
        _dataArray.setData(index, null, scn);
        _addrArray.setDual(index, HashFunction.NON_HASH_CODE, scn);
        _loadCount--;
        return true;
    }
    
    public final int getLevel()
    {
        return _level;
    }
    
    public final int getSplit()
    {
        return _split;
    }

    public final int getCapacity()
    {
        return _dataArray.length();
    }
    
    public final int getUnitCapacity()
    {
        return _unitCapacity;
    }
    
    public final int getLevelCapacity()
    {
        return _levelCapacity;
    }
    
    public final int getLoadCount()
    {
        return _loadCount;
    }
    
    public final double getLoadFactor()
    {
        return _loadCount / (double)getCapacity();
    }
    
    private void initLinearHashing() throws Exception
    {
        int unitCount = _dataArray.length() / getUnitCapacity();
        
        if(unitCount == 1)
        {
            _level = 0;
            _split = 0;
            _levelCapacity = getUnitCapacity();
        }
        else
        {
            // Determine level and split
            _level = 0;
            int remainder = (unitCount - 1) >> 1;
            while(remainder > 0)
            {
                _level++;
                remainder = remainder >> 1;
            }
            
            _split = (unitCount - (1 << _level) - 1) * getUnitCapacity();
            _levelCapacity = getUnitCapacity() * (1 << _level);
            
            // Need to re-populate the last unit
            for(int i = 0, cnt = getUnitCapacity(); i < cnt; i++)
            {
                forceSplit();
            }
        }
    }
    
    protected synchronized void trySplit() throws Exception
    {
        if(_split > 0)
        {
            forceSplit();
        }
    }
    
    protected synchronized void forceSplit() throws Exception
    {
        // Ensure address capacity
        _addrArray.expandCapacity(_split + _levelCapacity);
        
        // Read data from the _split index
        long address = _addrArray.get(_split);
        long hashCode = _addrArray.getDual(_split);
        
        int newCapacity = _levelCapacity << 1;
        int newIndex = (int)(hashCode % newCapacity);
        if (newIndex < 0) newIndex = -newIndex;
        
        if(newIndex != _split) /* Need to split */
        {
            long scn = nextScn();
            _addrArray.set(newIndex, address, hashCode, scn);
            _addrArray.set(_split, 0, HashFunction.NON_HASH_CODE, scn);
        }
        
        _split++;
        
        if(_split % _unitCapacity == 0)
        {
            _log.info("split " + getStatus());
        }
        
        if(_split == _levelCapacity)
        {
            _split = 0;
            _level++;
            _levelCapacity = getUnitCapacity() * (1 << _level);
            
            _log.info(getStatus());
        }
    }
    
    protected void rehash() throws Exception
    {
        do
        {
            trySplit();
        }
        while(_split > 0);
        sync();
    }
    
    protected int scan()
    {
        int cnt = 0;
        for(int i = 0, len = _dataArray.length(); i < len; i++)
        {
            if(_dataArray.hasData(i)) cnt++;
        }
        return cnt;
    }
    
    protected long nextScn()
    {
        return System.currentTimeMillis();
    }
    
    /**
     * @return the status of this data store.
     */
    public String getStatus()
    {
        StringBuffer buf = new StringBuffer();
        
        buf.append("level=");
        buf.append(_level);
        buf.append(" split=");
        buf.append(_split);
        buf.append(" capacity=");
        buf.append(getCapacity());
        buf.append(" loadCount=");
        buf.append(_loadCount);
        
        return buf.toString();
    }
    
    /**
     * @return the underlying data array.
     */
    public DataArray getDataArray()
    {
        return _dataArray;
    }
}
