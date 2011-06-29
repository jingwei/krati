package test.store.api;

import java.io.File;

import krati.core.StoreFactory;
import krati.core.StorePartitionConfig;
import krati.core.segment.Segment;
import krati.store.ArrayStore;
import test.util.RandomBytes;

/**
 * TestArrayStorePartitionApiConfig
 * 
 * @author jwu
 * 06/27, 2011
 */
public class TestArrayStorePartitionApiConfig extends AbstractTestArrayStoreApi {
    
    @Override
    protected ArrayStore createStore(File homeDir) throws Exception {
        int idStart = _rand.nextInt(100);
        int idCount = _rand.nextInt(100) + 1000;
        StorePartitionConfig config = new StorePartitionConfig(homeDir, idStart, idCount);
        config.setBatchSize(100);
        config.setNumSyncBatches(5);
        config.setSegmentFileSizeMB(Segment.minSegmentFileSizeMB);
        return StoreFactory.createArrayStorePartition(config);
    }
    
    public void testException() throws Exception {
        int index = _store.getIndexStart() + _store.capacity() + _rand.nextInt(100);
        byte[] value = RandomBytes.getBytes();
        
        try {
            _store.set(index, value, System.currentTimeMillis());
            assertFalse(true);
        } catch(ArrayIndexOutOfBoundsException e) {}
        
        try {
            _store.get(index);
            assertFalse(true);
        } catch(ArrayIndexOutOfBoundsException e) {}
    }
}

