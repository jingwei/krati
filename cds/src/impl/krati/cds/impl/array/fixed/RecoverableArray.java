package krati.cds.impl.array.fixed;

import java.io.File;
import java.io.IOException;
import java.util.List;

import krati.cds.array.Array;
import krati.cds.impl.array.entry.Entry;
import krati.cds.impl.array.entry.EntryFactory;
import krati.cds.impl.array.entry.EntryValue;

public interface RecoverableArray<V extends EntryValue> extends Array
{
  /**
   * @return the cache directory for storing array file and entry logs.
   */
  public File getCacheDirectory();
  
  /**
   * @return the array entry factory.
   */
  public EntryFactory<V> getEntryFactory();
  
  /**
   * Writes a list of entries to the array file on disk.
   * 
   * @param entryList
   */
  public void updateArrayFile(List<Entry<V>> entryList) throws IOException;
  
}
