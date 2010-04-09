package krati.sos;

import java.io.IOException;

import krati.cds.Persistable;

/**
 * An agent that wraps an ObjectCache can have inbound and outbound ObjectHandler(s).
 * The inbound handler is associated with the set method. It is called on an inbound object before the object is passed down to the underlying ObjectCache.
 * The outbound handler is associated with the get method. It is called on an outbound object before the object is returned back to the ObjectCache visitor.
 * Either inbound or outbound handlers does not affect the delete method.
 * 
 * <pre>
 *    get(int objectId)
 *      + get object from the underlying store
 *      + Call the outbound handler on the object
 *      + return the object
 *  
 *    set(int objectId, T object, long scn)
 *      + Call the inbound handler on the value object
 *      + delegate set to the underlying store
 * 
 * </pre>
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
}
