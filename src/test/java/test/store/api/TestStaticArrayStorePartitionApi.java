package test.store.api;

import java.io.File;

import test.util.RandomBytes;

import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.Segment;
import krati.store.ArrayStore;
import krati.store.StaticArrayStorePartition;

/**
 * TestStaticArrayStorePartition
 * 
 * @author jwu
 * 06/06, 2011
 * 
 */
public class TestStaticArrayStorePartitionApi extends AbstractTestArrayStoreApi {

    @Override
    protected ArrayStore createStore(File homeDir) throws Exception {
        int idStart = _rand.nextInt(100);
        int idCount = _rand.nextInt(100) + 1000;
        return new StaticArrayStorePartition(
                idStart,
                idCount,
                100,                    /* batchSize */
                5,                      /* numSyncBatches */
                homeDir,
                new MappedSegmentFactory(),
                Segment.minSegmentFileSizeMB,
                false);
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
