package krati.sos;

import java.io.IOException;

import krati.array.DynamicArray;
import krati.store.ArrayStorePartition;

/**
 * A simple data cache for serializable objects.
 * 
 * This class is not thread-safe by design. It is expected that the conditions below hold within one JVM.
 * <pre>
 *    1. There is one and only one instance of SerializableObjectCache for a given data cache.
 *    2. There is one and only one thread is calling set and delete methods at any given time. 
 * </pre>
 * 
 * It is expected that this class is used in the case of multiple readers and single writer.
 * 
 * @author jwu
 * 
 * @param <T> Serializable object.
 */
public class SerializableObjectCache<T> implements ObjectCache<T>
{
    protected final ArrayStorePartition _cache;
    protected final ObjectSerializer<T> _serializer;
    
    /**
     * Constructs an array-like object cache for serializable objects.
     * 
     * @param cache       the underlying data cache to store serializable objects.
     * @param serializer  the object serializer to serialize/de-serialize objects.
     */
    public SerializableObjectCache(ArrayStorePartition cache, ObjectSerializer<T> serializer)
    {
        this._cache = cache;
        this._serializer = serializer;
    }
    
    /**
     * @return the underlying data cache.
     */
    protected ArrayStorePartition getContentCache()
    {
        return _cache;
    }

    /**
     * @return the object serializer.
     */
    public ObjectSerializer<T> getSerializer()
    {
        return _serializer;
    }
    
    /**
     * @return the total number of objects in the cache.
     */
    @Override
    public int getObjectIdCount()
    {
        return _cache.getIdCount();
    }
    
    /**
     * @return the start of ObjectId(s) allowed by the cache.
     */
    @Override
    public int getObjectIdStart()
    {
        return _cache.getIdStart();
    }
    
    /**
     * Gets an object based on a user-specified object Id.
     * 
     * @param objectId    the Id of an object to be retrieved from the cache. 
     * @return an object associated with the given objectId.
     */
    @Override
    public T get(int objectId)
    {
        return getSerializer().construct(_cache.get(objectId));
    }
    
    /**
     * Sets an object at a user-specified object Id.
     * 
     * @param objectId    the object Id.
     * @param object      the object to put into the cache.
     * @param scn         the global scn (equivalent to a time stamp).
     * @throws Exception
     */
    @Override
    public boolean set(int objectId, T object, long scn) throws Exception
    {
        if(object == null)
        {
            return delete(objectId, scn);
        }
        
        _cache.set(objectId, getSerializer().serialize(object), scn);
        return true;
    }
    
    /**
     * Deletes an object based on a user-specified object Id.
     * 
     * @param objectId   the object Id.
     * @param scn        the global scn (equivalent to a time stamp).
     * @throws Exception
     */
    @Override
    public boolean delete(int objectId, long scn) throws Exception
    {
        _cache.delete(objectId, scn);
        return true;
    }

    /**
     * Sync this object cache.
     * 
     * @throws IOException
     */
    @Override
    public void sync() throws IOException
    {
        synchronized(_cache)
        {
            _cache.sync();
        }
    }
    
    /**
     * Persists this object cache.
     * 
     * @throws IOException
     */
    @Override
    public void persist() throws IOException
    {
        synchronized(_cache)
        {
            _cache.persist();
        }
    }
    
    /**
     * Clears this object cache by removing all the persisted data permanently.
     * 
     * @throws IOException
     */
    @Override
    public void clear()
    {
        synchronized(_cache)
        {
            _cache.clear();
        }
    }
    
    /**
     * @return the high water mark.
     */
    @Override
    public long getHWMark()
    {
        return _cache.getHWMark();
    }
    
    /**
     * @return the low water mark.
     */
    @Override
    public long getLWMark()
    {
        return _cache.getLWMark();
    }
    
    /**
     * Saves the high water mark.
     */
    @Override
    public void saveHWMark(long endOfPeriod) throws Exception
    {
        _cache.saveHWMark(endOfPeriod);
    }
    
    /**
     * Gets an object in raw bytes based on a user-specified object Id.
     * 
     * @param objectId    the Id of an object to be retrieved from the cache. 
     * @return            an object in raw bytes according to the given object Id.
     */
    @Override
    public byte[] getBytes(int objectId)
    {
        return _cache.get(objectId);
    }

    @Override
    public void expandCapacity(int index) throws Exception {
        if(this instanceof DynamicArray) {
            ((DynamicArray)this).expandCapacity(index);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public boolean hasIndex(int index) {
        return _cache.hasIndex(index);
    }

    @Override
    public int length() {
        return _cache.length();
    }
}
