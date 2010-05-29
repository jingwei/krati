package krati.cds.impl.array;

import krati.cds.Persistable;
import krati.cds.array.LongArray;
import krati.cds.impl.array.entry.EntryPersistListener;

/**
 * AddressArray is for maintaining address pointers (long values) to data stored in segments. 
 * 
 * @author jwu
 *
 */
public interface AddressArray extends LongArray, Persistable
{
    /**
     * Gets the listener that is called whenever an entry is persisted. 
     */
    public EntryPersistListener getPersistListener();
    
    /**
     * Sets the listener that is called whenever an entry is persisted.
     * @param persistListener
     */
    public void setPersistListener(EntryPersistListener persistListener);
}
