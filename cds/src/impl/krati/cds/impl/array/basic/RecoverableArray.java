package krati.cds.impl.array.basic;

import java.io.File;
import java.io.IOException;
import java.util.List;

import krati.cds.Persistable;
import krati.cds.array.Array;
import krati.cds.impl.array.entry.Entry;
import krati.cds.impl.array.entry.EntryFactory;
import krati.cds.impl.array.entry.EntryValue;

public interface RecoverableArray<V extends EntryValue> extends Array, Persistable
{
    public File getDirectory();
    
    public EntryFactory<V> getEntryFactory();
    
    public void updateArrayFile(List<Entry<V>> entryList) throws IOException;
}
