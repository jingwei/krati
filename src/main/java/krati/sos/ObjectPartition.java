package krati.sos;

/**
 * ObjectPartition
 * 
 * @author jwu
 * 01/15, 2011
 * 
 * @param <T>
 */
public interface ObjectPartition<T> extends ObjectArray<T> {
    
    /**
     * @return the total number of objects in the partition.
     */
    public int getObjectIdCount();
    
    /**
     * @return the start of ObjectId(s) allowed by the partition.
     */
    public int getObjectIdStart();
}
