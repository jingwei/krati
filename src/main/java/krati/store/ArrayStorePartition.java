package krati.store;

/**
 * ArrayStorePartition - Partitioned array for storing bytes (i.e. member data) at specified indexes (i.e. member ID).
 * 
 * @author jwu
 * 01/10, 2011
 *
 */
public interface ArrayStorePartition extends ArrayStore {
    
    public int getIdCount();
    
    public int getIdStart();
}
