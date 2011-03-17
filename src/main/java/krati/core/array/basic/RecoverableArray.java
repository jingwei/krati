package krati.core.array.basic;

import java.io.File;
import java.io.IOException;
import java.util.List;

import krati.Persistable;
import krati.array.Array;
import krati.core.array.entry.Entry;
import krati.core.array.entry.EntryFactory;
import krati.core.array.entry.EntryValue;

/**
 * RecoverableArray
 * 
 * @author jwu
 * 
 */
public interface RecoverableArray<V extends EntryValue> extends Array, Persistable {
    
    public File getDirectory();
    
    public EntryFactory<V> getEntryFactory();
    
    public void updateArrayFile(List<Entry<V>> entryList) throws IOException;
}
