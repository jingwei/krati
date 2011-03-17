package krati.core.array.entry;

import java.io.IOException;

/**
 * EntryPersistAdapter
 * 
 * @author jwu
 * 
 */
public class EntryPersistAdapter implements EntryPersistListener {
    
    @Override
    public void beforePersist(Entry<? extends EntryValue> e) throws IOException {}
    
    @Override
    public void afterPersist(Entry<? extends EntryValue> e) throws IOException {}
}
