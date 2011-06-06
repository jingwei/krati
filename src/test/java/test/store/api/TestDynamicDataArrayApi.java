package test.store.api;

import java.io.File;
import java.util.Arrays;

import test.util.RandomBytes;

import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.Segment;
import krati.store.ArrayStore;
import krati.store.DynamicDataArray;

/**
 * TestDynamicDataArrayApi
 * 
 * @author jwu
 * 06/06, 2011
 * 
 */
public class TestDynamicDataArrayApi extends AbstractTestArrayStoreApi {

    @Override
    protected ArrayStore createStore(File homeDir) throws Exception {
        return new DynamicDataArray(
                _rand.nextInt(1 << 20), /* length */
                100,                    /* batchSize */
                5,                      /* numSyncBatches */
                homeDir,
                new MappedSegmentFactory(),
                Segment.minSegmentFileSizeMB,
                Segment.defaultSegmentCompactFactor);
    }
    
    public void testAutoExpand() throws Exception {
        int index = _store.getIndexStart() + _store.capacity() + _rand.nextInt(100);
        byte[] value = RandomBytes.getBytes();
        
        int oldCapacity = _store.capacity();
        _store.set(index, value, System.currentTimeMillis());
        assertTrue(Arrays.equals(value, _store.get(index)));;
        int newCapacity = _store.capacity();
        assertTrue(oldCapacity < newCapacity);
    }
}

