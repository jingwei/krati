package krati.core;

import java.io.File;
import java.io.IOException;

/**
 * StorePartitionConfig
 * 
 * @author jwu
 * 06/26, 2011
 * 
 */
public class StorePartitionConfig extends StoreConfig {
    private final int _partitionCount;
    private final int _partitionStart;
    private final int _partitionEnd;
    
    public StorePartitionConfig(File homeDir, int partitionStart, int partitionCount) throws IOException {
        super(homeDir, partitionStart, partitionCount);
        this._partitionStart = partitionStart;
        this._partitionCount = partitionCount;
        this._partitionEnd = partitionStart + partitionCount;
    }
    
    public final int getPartitionCount() {
        return _partitionCount;
    }
    
    public final int getPartitionStart() {
        return _partitionStart;
    }
    
    public final int getPartitionEnd() {
        return _partitionEnd;
    }
}
