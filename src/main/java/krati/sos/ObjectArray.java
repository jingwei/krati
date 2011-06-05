package krati.sos;

import krati.Persistable;
import krati.array.DynamicArray;
import krati.io.Closeable;

/**
 * ObjectArray
 * 
 * @author jwu
 * 01/15, 2011
 * 
 * @param <T>
 * 
 * 06/04, 2011 - Added interface Closeable
 */
public interface ObjectArray<T> extends Persistable, Closeable, DynamicArray {

    /**
     * Gets an object based on a user-specified object Id.
     * 
     * @param objectId    the Id of an object to be retrieved from the array. 
     * @return an object associated with the given objectId.
     */
    public T get(int objectId);
    
    /**
     * Gets an object in raw bytes based on a user-specified object Id.
     * 
     * @param objectId    the Id of an object to be retrieved from the array. 
     * @return            an object in raw bytes according to the given object Id.
     */
    public byte[] getBytes(int objectId);
    
    /**
     * Sets an object at a user-specified object Id.
     * 
     * @param objectId    the object Id.
     * @param object      the object to put into the array.
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
    
}
