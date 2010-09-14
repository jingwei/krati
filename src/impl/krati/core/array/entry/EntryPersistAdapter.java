package krati.core.array.entry;

import java.io.IOException;

public class EntryPersistAdapter implements EntryPersistListener
{
    @Override
    public void priorPersisting(Entry<? extends EntryValue> e) throws IOException {}
    
    @Override
    public void afterPersisting(Entry<? extends EntryValue> e) throws IOException {}
}
