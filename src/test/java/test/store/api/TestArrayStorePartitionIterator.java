package test.store.api;

import java.io.File;

import krati.core.StoreFactory;
import krati.core.StorePartitionConfig;
import krati.store.ArrayStore;

/**
 * TestArrayStorePartitionIterator
 * 
 * @author jwu
 * @since 10/08, 2011
 */
public class TestArrayStorePartitionIterator extends AbstractTestArrayStoreIterator {
    
    @Override
    protected ArrayStore createStore(File homeDir) throws Exception {
        StorePartitionConfig config = new StorePartitionConfig(homeDir, 101, 1000);
        return StoreFactory.createArrayStorePartition(config);
    }
}