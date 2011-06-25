package test.store;

import krati.core.segment.SegmentFactory;

/**
 * TestIndexedStore using WriteBufferSegment.
 * 
 * @author jwu
 *
 */
public class TestIndexedStoreWriteBuffer extends TestIndexedStore {
    @Override
    protected SegmentFactory createStoreSegmentFactory() {
        return new krati.core.segment.WriteBufferSegmentFactory();
    }
}
