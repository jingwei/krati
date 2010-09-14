package krati.util;

import java.io.File;
import java.io.IOException;

import krati.store.DataCache;

/**
 * DataCacheLoader
 * 
 * @author jwu
 *
 */
public interface DataCacheLoader
{
    public void load(DataCache cache, File dataFile) throws IOException;
    
    public void dump(DataCache cache, File dumpFile) throws IOException;
}
