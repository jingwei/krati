package krati.sos;

/**
 * ObjectArrayPartition
 * 
 * @author jwu
 * 01/15, 2011
 * 
 * @param <T>
 */
public interface ObjectArrayPartition<T> extends ObjectArray<T> {
    
    /**
     * @return the total number of objects in the cache.
     */
    public int getObjectIdCount();
    
    /**
     * @return the start of ObjectId(s) allowed by the cache.
     */
    public int getObjectIdStart();
}
