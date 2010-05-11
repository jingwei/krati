package krati.cds.impl.store;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import krati.cds.DataCache;
import krati.cds.store.DataStore;
import krati.util.FnvHashFunction;
import krati.util.HashFunction;

/**
 * A simple implementation of key value store.
 * 
 * The key-value pairs are stored in DataCache using the following data format:
 * <pre>
 * [count][key-length][key][value-length][value][key-length][key][value-length][value]...
 *        +---------key-value pair 1-----------+----------key-value pair 2-----------+
 * </pre>
 * 
 * @author jwu
 *
 */
public class SimpleDataStore implements DataStore<byte[], byte[]>
{
    private final static Logger _log = Logger.getLogger(SimpleDataStore.class);
    
    private long _scn;
    private final DataCache _cache;
    private final HashFunction<byte[]> _hashFunction;
    
    public SimpleDataStore(DataCache cache)
    {
        this(cache, new FnvHashFunction());
    }
    
    public SimpleDataStore(DataCache cache, HashFunction<byte[]> hashFunc)
    {
        this._cache = cache;
        this._hashFunction = hashFunc;
        this._scn = _cache.getLWMark();
    }
    
    @Override
    public void sync() throws IOException
    {
        _cache.sync();
    }
    
    @Override
    public void persist() throws IOException
    {
        _cache.persist();
    }
    
    @Override
    public long hash(byte[] key)
    {
        return _hashFunction.hash(key);
    }
    
    @Override
    public byte[] get(byte[] key)
    {
        long hashCode = hash(key);
        int index = _cache.getIdStart() + (int)(hashCode % _cache.getIdCount());
        
        byte[] existingData = _cache.getData(index);
        return existingData == null ? null : extractByKey(key, existingData);
    }
    
    @Override
    public synchronized boolean put(byte[] key, byte[] value) throws Exception
    {
        if(value == null) return delete(key);
        
        long hashCode = hash(key);
        int index = _cache.getIdStart() + (int)(hashCode % _cache.getIdCount());
        
        byte[] existingData = _cache.getData(index);
        if(existingData == null || existingData.length == 0)
        {
            _cache.setData(index, assemble(key, value), _scn++);
        }
        else
        {
            try
            {
                _cache.setData(index, assemble(existingData, key, value), _scn++);
            }
            catch(Exception e)
            {
                _log.warn("Value reset at index="+ index + " key=\"" + new String(key) + "\"");
                _cache.setData(index, assemble(key, value), _scn++);
            }
        }
        
        return true;
    }
    
    @Override
    public synchronized boolean delete(byte[] key) throws Exception
    {
        long hashCode = hash(key);
        int index = _cache.getIdStart() + (int)(hashCode % _cache.getIdCount());
        
        try
        {
            byte[] existingData = _cache.getData(index);
            if(existingData != null)
            {
               int newLength = removeByKey(key, existingData);
               if(newLength == 0)
               {
                   // entire data is removed
                   _cache.deleteData(index, _scn++);
                   return true;
               }
               else if(newLength < existingData.length)
               {
                   // partial data is removed
                   _cache.setData(index, existingData, 0, newLength, _scn++);
                   return true;
               }
            }
        }
        catch(Exception e)
        {
            _log.warn("Failed to delete key=\""+ new String(key) + "\" : " + e.getMessage());
            _cache.deleteData(index, _scn++);
        }
        
        // no data is removed
        return false;
    }
    
    @Override
    public synchronized void clear() throws IOException
    {
        _cache.clear();
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
}
