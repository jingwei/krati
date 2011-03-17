package krati.util;

import java.io.File;
import java.io.IOException;

import krati.store.ArrayStorePartition;

/**
 * PartitionLoader
 * 
 * @author jwu
 *
 */
public interface PartitionLoader {
    
    public void load(ArrayStorePartition p, File dataFile) throws IOException;
    
    public void dump(ArrayStorePartition p, File dumpFile) throws IOException;
}
