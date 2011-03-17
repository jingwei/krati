package krati.core.array.entry;

import java.io.IOException;

/**
 * Listener for persist events.
 * 
 * @author jwu
 * 
 */
public interface EntryPersistListener {
    
    public void beforePersist(Entry<? extends EntryValue> e) throws IOException;
    
    public void afterPersist(Entry<? extends EntryValue> e) throws IOException;
}
