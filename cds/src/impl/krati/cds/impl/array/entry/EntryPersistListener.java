package krati.cds.impl.array.entry;

import java.io.IOException;

public interface EntryPersistListener {
    public void priorPersisting(Entry<? extends EntryValue> e) throws IOException;
    public void afterPersisting(Entry<? extends EntryValue> e) throws IOException;
}
