package krati.cds.impl.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import krati.cds.impl.array.SimpleDataArray;
import krati.cds.impl.array.basic.DynamicLongArray;
import krati.cds.impl.segment.SegmentFactory;
import krati.cds.impl.segment.SegmentManager;
import krati.cds.store.DataStore;
import krati.util.FnvHashFunction;
import krati.util.HashFunction;

/**
 * DynamicDataStore is implemented using Linear Hashing. Its capacity grows as needed.
 * 
 * The key-value pairs are stored in the underlying DataArray using the following format:
 * <pre>
 * [count][key-length][key][value-length][value][key-length][key][value-length][value]...
 *        +---------key-value pair 1-----------+----------key-value pair 2-----------+
 * </pre>
 * 
 * @author jwu
 *
 */
public class DynamicDataStore implements DataStore<byte[], byte[]>
{
    private final static Logger _log = Logger.getLogger(DynamicDataStore.class);
    
    private long _scn;
    private final double _loadFactor;
    private final SimpleDataArray _dataArray;
    private final DynamicLongArray _addrArray;
    private final HashFunction<byte[]> _hashFunction;
    private volatile int _split;
    private volatile int _level;
    private volatile int _levelCapacity;
    private int _unitCapacity;
    private int _loadCount;
    private int _loadLimit;
    
    /**
     * Creates a dynamic DataStore with the settings below:
     * 
     * <pre>
     *    Initial Level            : 0
     *    Entry Size               : 10000
     *    Max Entries              : 5
     *    Segment File Size        : 256MB
     *    Segment Compact Trigger  : 0.1
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
    public DynamicDataStore(File homeDir, SegmentFactory segmentFactory) throws Exception
    {
        this(homeDir,
             0,     /* initial level */ 
             10000, /* entrySize */
             5,     /* maxEntries */
             256,   /* segmentFileSizeMB */
             segmentFactory,
             0.1,   /* segmentCompactTrigger */
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
     *    Segment Compact Trigger  : 0.1
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
    public DynamicDataStore(File homeDir,
                            int initLevel,
                            SegmentFactory segmentFactory) throws Exception
    {
        this(homeDir,
             initLevel,
             10000, /* entrySize */
             5,     /* maxEntries */
             256,   /* segmentFileSizeMB */
             segmentFactory,
             0.1,   /* segmentCompactTrigger */
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
     *    Segment Compact Trigger  : 0.1
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
    public DynamicDataStore(File homeDir,
                            int initLevel,
                            int segmentFileSizeMB,
                            SegmentFactory segmentFactory) throws Exception
    {
        this(homeDir,
             initLevel,
             10000, /* entrySize */
             5,     /* maxEntries */
             segmentFileSizeMB,
             segmentFactory,
             0.1,   /* segmentCompactTrigger */
             0.5,   /* segmentCompactFactor  */
             0.75,  /* DataStore load factor */
             new FnvHashFunction());
    }
    
    /**
     * Creates a dynamic DataStore with the settings below:
     * 
     * <pre>
     *    Segment Compact Trigger  : 0.1
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
    public DynamicDataStore(File homeDir,
                            int initLevel,
                            int entrySize,
                            int maxEntries,
                            int segmentFileSizeMB,
                            SegmentFactory segmentFactory) throws Exception
    {
        this(homeDir,
             initLevel,
             entrySize,
             maxEntries,
             segmentFileSizeMB,
             segmentFactory,
             0.1,   /* segmentCompactTrigger */
             0.5,   /* segmentCompactFactor  */
             0.75,  /* DataStore load factor */
             new FnvHashFunction());
    }
    
    /**
     * Creates a dynamic DataStore.
     * 
     * <pre>
     *    Segment Compact Trigger  : 0.1
     *    Segment Compact Factor   : 0.5
     *    Store Hash Load Factor   : 0.75
     * </pre>
     * 
     * @param homeDir                the home directory of DataStore
     * @param initLevel              the initial level when DataStore is created
     * @param entrySize              the redo entry size (i.e., batch size)
     * @param maxEntries             the number of redo entries required for updating the underlying address array
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @param hashFunction           the hash function for mapping keys to indexes
     * @throws Exception             if this dynamic data store cannot be created.
     */
    public DynamicDataStore(File homeDir,
                            int initLevel,
                            int entrySize,
                            int maxEntries,
                            int segmentFileSizeMB,
                            SegmentFactory segmentFactory,
                            HashFunction<byte[]> hashFunction) throws Exception
    {
        this(homeDir,
             initLevel,
             entrySize,
             maxEntries,
             segmentFileSizeMB,
             segmentFactory,
             0.1,   /* segmentCompactTrigger */
             0.5,   /* segmentCompactFactor  */
             0.75,  /* DataStore load factor */
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
     * @param segmentCompactTrigger  the percentage of segment capacity, which triggers compaction once per segment
     * @param segmentCompactFactor   the load factor of segment, below which a segment is eligible for compaction
     * @param hashLoadFactor         the load factor of the underlying address array (which works as a hash table)
     * @param hashFunction           the hash function for mapping keys to indexes
     * @throws Exception             if this dynamic data store cannot be created.
     */
    public DynamicDataStore(File homeDir,
                            int initLevel,
                            int entrySize,
                            int maxEntries,
                            int segmentFileSizeMB,
                            SegmentFactory segmentFactory,
                            double segmentCompactTrigger,
                            double segmentCompactFactor,
                            double hashLoadFactor,
                            HashFunction<byte[]> hashFunction) throws Exception
    {
        // Create dynamic address array
        _addrArray = createAddressArray(entrySize, maxEntries, homeDir);
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
        this._scn = _dataArray.getLWMark();
        this._loadFactor = hashLoadFactor;
        this._loadCount = scan();
        this.initLinearHashing();
        
        _log.info("init: " + getStatus());
    }
    
    protected DynamicLongArray createAddressArray(int entrySize,
                                                  int maxEntries,
                                                  File homeDirectory) throws Exception
    {
        _unitCapacity = DynamicLongArray.subArrayLength();
        return new DynamicLongArray(entrySize, maxEntries, homeDirectory);
    }
    
    protected long hash(byte[] key)
    {
        return _hashFunction.hash(key);
    }
    
    @Override
    public void sync() throws IOException
    {
        _dataArray.sync();
    }
    
    @Override
    public void persist() throws IOException
    {
        _dataArray.persist();
    }
    
    @Override
    public byte[] get(byte[] key)
    {
        byte[] existingData;
        long hashCode = hash(key);
        
        /**
         * Need SPIN to retrieve data from the underlying array
         * because the index might have changed with the _split. 
         */
        
        // Map key to an array index
        int index = getIndex(hashCode);
        
        do
        {
            // Read existing data at the index
            existingData = _dataArray.getData(index);
            
            // Check that key is still mapped to the known index
            int indexNew = getIndex(hashCode);
            if(index == indexNew) break;
            else index = indexNew;
        } while(true);
        
        return existingData == null ? null : extractByKey(key, existingData);
    }
    
    public synchronized boolean put(byte[] key, byte[] value) throws Exception
    {
        if(value == null)
        {
            return delete(key);
        }
        
        if(0 < _split || _loadLimit < _loadCount)
        {
            split();
        }
        
        int index = getIndex(key);
        return putInternal(index, key, value);
    }
    
    @Override
    public synchronized boolean delete(byte[] key) throws Exception
    {
        if(0 < _split || _loadLimit < _loadCount)
        {
            split();
        }
        
        int index = getIndex(key);
        return deleteInternal(index, key);
    }
    
    @Override
    public synchronized void clear() throws IOException
    {
        _dataArray.clear();
    }
    
    protected final int getIndex(byte[] key)
    {
        long hashCode = hash(key);
        int capacity = _levelCapacity;
        int index = (int)(hashCode % capacity);
        if (index < _split)
        {
            capacity = capacity << 1;
            index = (int)(hashCode % capacity);
        }
        
        return index;
    }
    
    protected final int getIndex(long hashCode)
    {
        int capacity = _levelCapacity;
        int index = (int)(hashCode % capacity);
        if (index < _split)
        {
            capacity = capacity << 1;
            index = (int)(hashCode % capacity);
        }
        
        return index;
    }
    
    protected int removeByKey(byte[] key, byte[] data)
    {
        int offset1 = 0;
        int offset2 = 0;
        ByteBuffer bb = ByteBuffer.wrap(data);
        
        int originalCnt = bb.getInt();
        int cnt = originalCnt;
        while(cnt > 0)
        {
            offset1 = bb.position();
            
            // Process key
            int len = bb.getInt();
            if(keysEqual(key, data, bb.position(), len))
            {
                bb.position(bb.position() + len);
                
                // Process value
                len = bb.getInt();
                bb.position(bb.position() + len);
                
                offset2 = bb.position();
                break;
            }
            bb.position(bb.position() + len);
            
            // Process value
            len = bb.getInt();
            bb.position(bb.position() + len);
            
            cnt--;
        }
        
        // key is found and remove key-value from data
        if(offset1 < offset2)
        {
            int newLength = data.length - (offset2 - offset1);
            
            /*
             * entire data is removed
             */
            if(newLength <= 4) return 0;
            
            /*
             * partial data is removed
             */
            
            // update data count
            bb.position(0);
            bb.putInt(originalCnt - 1);
            
            // Shift data to the left
            for(int i = 0, len = data.length - offset2; i < len; i++)
            {
                data[offset1 + i] = data[offset2 + i];
            }
            
            return newLength;
        }
        
        // no data is removed
        return data.length;
    }
    
    protected byte[] assemble(byte[] key, byte[] value)
    {
        byte[] result = new byte[4 + 4 + key.length + 4 + value.length];
        ByteBuffer bb = ByteBuffer.wrap(result);
        
        // count
        bb.putInt(1);
        
        // add key
        bb.putInt(key.length);
        bb.put(key);
        
        // add value
        bb.putInt(value.length);
        bb.put(value);
        
        return result;
    }
    
    protected byte[] assemble(byte[] existingData, byte[] key, byte[] value)
    {
        // Remove old data
        int newLength = removeByKey(key, existingData);
        if(newLength == 0) return assemble(key, value);
        
        byte[] result = new byte[newLength + 4 + key.length + 4 + value.length];
        System.arraycopy(existingData, 0, result, 0, newLength);
        
        ByteBuffer bb = ByteBuffer.wrap(result);
        
        int cnt = bb.getInt();
        
        // update count
        bb.position(0);
        bb.putInt(cnt + 1);
        
        // add key
        bb.position(newLength);
        bb.putInt(key.length);
        bb.put(key);
        
        // add value
        bb.putInt(value.length);
        bb.put(value);
        
        return result;
    }
    
    protected byte[] extractByKey(byte[] key, byte[] data)
    {
        if(data.length == 0) return null;
        ByteBuffer bb = ByteBuffer.wrap(data);
        
        int cnt = bb.getInt();
        while(cnt > 0)
        {
            // Process key
            int len = bb.getInt();
            if(keysEqual(key, data, bb.position(), len))
            {
                // pass key data
                bb.position(bb.position() + len);
                
                // Process value
                len = bb.getInt();
                byte[] result = new byte[len];
                bb.get(result);
                
                return result;
            }
            bb.position(bb.position() + len);
            
            // Process value
            len = bb.getInt();
            bb.position(bb.position() + len);
            
            cnt--;
        }
        
        // no data is found for the key
        return null;
    }
    
    protected boolean keysEqual(byte[] key, byte[] keySource, int offset, int length)
    {
        if(key.length == length)
        {
            for(int i = 0; i < length; i++)
            {
                if(key[i] != keySource[offset + i]) return false;
            }
            return true;
        }
        
        return false;
    }
    
    protected boolean putInternal(int index, byte[] key, byte[] value) throws Exception
    {
        byte[] existingData = _dataArray.getData(index);
        if(existingData == null || existingData.length == 0)
        {
            _dataArray.setData(index, assemble(key, value), _scn++);
            _loadCount++;
        }
        else
        {
            try
            {
                _dataArray.setData(index, assemble(existingData, key, value), _scn++);
            }
            catch(Exception e)
            {
                _log.warn("Value reset at index="+ index + " key=\"" + new String(key) + "\"");
                _dataArray.setData(index, assemble(key, value), _scn++);
            }
        }
        
        return true;
    }
    
    protected boolean deleteInternal(int index, byte[] key) throws Exception
    {
        try
        {
            byte[] existingData = _dataArray.getData(index);
            if(existingData != null)
            {
               int newLength = removeByKey(key, existingData);
               if(newLength == 0)
               {
                   // entire data is removed
                   _dataArray.setData(index, null, _scn++);
                   _loadCount--;
                   return true;
               }
               else if(newLength < existingData.length)
               {
                   // partial data is removed
                   _dataArray.setData(index, existingData, 0, newLength, _scn++);
                   return true;
               }
            }
        }
        catch(Exception e)
        {
            _log.warn("Failed to delete key=\""+ new String(key) + "\" : " + e.getMessage());
            _dataArray.setData(index, null, _scn++);
        }
        
        // no data is removed
        return false;
    }
    
    protected final int getLevel()
    {
        return _level;
    }
    
    protected final int getSplit()
    {
        return _split;
    }
    
    protected final int getUnitCapacity()
    {
        return _unitCapacity;
    }
    
    private void initLinearHashing() throws Exception
    {
        int unitCount = _dataArray.length() / getUnitCapacity();
        
        if(unitCount == 1)
        {
            _level = 0;
            _split = 0;
            _levelCapacity = getUnitCapacity();
            _loadLimit = (int)(_levelCapacity * _loadFactor);
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
            _loadLimit = (int)((_levelCapacity << 1) * _loadFactor);
            
            // Need to re-populate the last unit
            for(int i = 0, cnt = getUnitCapacity(); i < cnt; i++)
            {
                split();
            }
        }
        
    }
    
    private void split() throws Exception
    {
        // Read data from the _split index
        byte[] data = _dataArray.getData(_split);
        
        // Process read data
        if (data != null && data.length > 0)
        {
            ByteBuffer bb = ByteBuffer.wrap(data);
            int newCapacity = _levelCapacity << 1;
            
            int cnt = bb.getInt();
            while(cnt > 0)
            {
                // Read key
                int len = bb.getInt();
                byte[] key = new byte[len];
                bb.get(key);
                
                int newIndex = (int)(hash(key) % newCapacity);
                if(newIndex == _split) /* No need to split */
                {
                    // Pass value
                    len = bb.getInt();
                    bb.position(bb.position() + len);
                }
                else
                {
                    // Read value
                    len = bb.getInt();
                    byte[] value = new byte[len];
                    bb.get(value);
                    
                    // Remove at the old index
                    deleteInternal(_split, key);
                    
                    // Update at the new index
                    _addrArray.expandCapacity(newIndex);
                    putInternal(newIndex, key, value);
                }
                
                cnt--;
            }
        }
        
        _split++;

        if(_split % _unitCapacity == 0)
        {
            _log.info("split info: " + getStatus());
        }
        
        if(_split == _levelCapacity)
        {
            _split = 0;
            _level++;
            _levelCapacity = getUnitCapacity() * (1 << _level);
            _loadLimit = (int)((_levelCapacity << 1) * _loadFactor);
            
            _log.info("split done: " + getStatus());
        }
    }
    
    private int scan()
    {
        int cnt = 0;
        for(int i = 0, len = _dataArray.length(); i < len; i++)
        {
            if(_dataArray.hasData(i)) cnt++;
        }
        return cnt;
    }
    
    public String getStatus()
    {
        StringBuffer buf = new StringBuffer();
        
        buf.append("level=");
        buf.append(_level);
        buf.append(" split=");
        buf.append(_split);
        buf.append(" scn=");
        buf.append(_scn);
        buf.append(" loadCount=");
        buf.append(_loadCount);
        buf.append(" loadLimit=");
        buf.append(_loadLimit);
        buf.append(" loadFactor=");
        buf.append(_loadFactor);
        
        return buf.toString();
    }
}
