package krati.store;

/**
 * ArrayStorePartition - Partitioned array for storing bytes (e.g., member information) at specified indexes (i.e. member ID).
 * 
 * @author jwu
 * 01/10, 2011
 * 
 */
public interface ArrayStorePartition extends ArrayStore {
    
    /**
     * @return the number of Id(s) of this partition.
     */
    public int getIdCount();
    
    /**
     * @return the start Id of this partition.
     */
    public int getIdStart();
}
