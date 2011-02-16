package test.store;

import krati.core.segment.SegmentFactory;

/**
 * TestIndexedStore using MappedSegment.
 * 
 * @author jwu
 *
 */
public class TestIndexedStoreMapped extends TestIndexedStore {
    @Override
    protected SegmentFactory createStoreSegmentFactory() {
        return new krati.core.segment.MappedSegmentFactory();
    }
}
