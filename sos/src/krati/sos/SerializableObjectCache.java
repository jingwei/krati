package krati.sos;

import java.io.IOException;

import krati.cds.DataCache;

/**
 * A simple data cache for serializable objects.
 * 
 * @author jwu
 *
 * @param <T> Serializable object.
 */
public class SerializableObjectCache<T> implements ObjectCache<T>
{
    protected final DataCache _cache;
    protected final ObjectSerializer<T> _serializer;
    
    /**
     * Constructs an array-like object cache for serializable objects.
     * 
     * @param cache       the underlying data cache to store serializable objects.
     * @param serializer  the object serializer to serialize/de-serialize objects.
     */
    public SerializableObjectCache(DataCache cache, ObjectSerializer<T> serializer)
    {
        this._cache = cache;
        this._serializer = serializer;
    }
    
    /**
     * @return the underlying data cache.
     */
    protected DataCache getContentCache()
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
        return getSerializer().construct(_cache.getData(objectId));
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
    public void set(int objectId, T object, long scn) throws Exception
    {
        _cache.setData(objectId, getSerializer().serialize(object), scn);
    }
    
    /**
     * Deletes an object based on a user-specified object Id.
     * 
     * @param objectId   the object Id.
     * @param scn        the global scn (equivalent to a time stamp).
     * @throws Exception
     */
    @Override
    public void delete(int objectId, long scn) throws Exception
    {
        _cache.deleteData(objectId, scn);
    }

    /**
     * Persists this object cache.
     * 
     * @throws IOException
     */
    @Override
    public void persist() throws IOException
    {
        _cache.persist();
    }
}
