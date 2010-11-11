package krati.sos;

import java.io.IOException;

import krati.Persistable;

/**
 * ObjectCache
 * 
 * @author jwu
 *
 * @param <T> Object to be cached.
 */
public interface ObjectCache<T> extends Persistable
{
    /**
     * @return the total number of objects in the cache.
     */
    public int getObjectIdCount();
    
    /**
     * @return the start of ObjectId(s) allowed by the cache.
     */
    public int getObjectIdStart();
    
    /**
     * Gets an object based on a user-specified object Id.
     * 
     * @param objectId    the Id of an object to be retrieved from the cache. 
     * @return an object associated with the given objectId.
     */
    public T get(int objectId);
    
    /**
     * Gets an object in raw bytes based on a user-specified object Id.
     * 
     * @param objectId    the Id of an object to be retrieved from the cache. 
     * @return            an object in raw bytes according to the given object Id.
     */
    public byte[] getBytes(int objectId);
    
    /**
     * Sets an object at a user-specified object Id.
     * 
     * @param objectId    the object Id.
     * @param object      the object to put into the cache.
     * @param scn         the global scn (equivalent to a time stamp).
     * @throws Exception
     */
    public boolean set(int objectId, T object, long scn) throws Exception;
    
    /**
     * Deletes an object based on a user-specified object Id.
     * 
     * @param objectId   the object Id.
     * @param scn        the global scn (equivalent to a time stamp).
     * @throws Exception
     */
    public boolean delete(int objectId, long scn) throws Exception;
    
    /**
     * Persists this object cache.
     * 
     * @throws IOException
     */
    public void persist() throws IOException;
    
    /**
     * Clears this object cache by removing all the persisted data permanently.
     * 
     * @throws IOException
     */
    public void clear() throws IOException;
}
