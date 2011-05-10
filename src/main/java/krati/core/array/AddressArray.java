package krati.core.array;

import krati.Persistable;
import krati.array.LongArray;
import krati.core.array.entry.EntryPersistListener;
import krati.io.Closeable;

/**
 * AddressArray is for maintaining address pointers (long values) to data stored in segments. 
 * 
 * @author jwu
 *
 */
public interface AddressArray extends LongArray, Persistable, Closeable {
    
    /**
     * Gets the listener that is called whenever an entry is persisted. 
     */
    public EntryPersistListener getPersistListener();
    
    /**
     * Sets the listener that is called whenever an entry is persisted.
     * @param persistListener
     */
    public void setPersistListener(EntryPersistListener persistListener);
    
    /**
     * Sets the compaction address (produced by a compactor) at a specified index.
     * 
     * @param index   - the index to address array
     * @param address - the address value for update
     * @param scn     - the scn associated with this change
     */
    public void setCompactionAddress(int index, long address, long scn) throws Exception;
}
