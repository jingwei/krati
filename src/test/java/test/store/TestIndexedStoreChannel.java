package test.store;

import krati.core.segment.SegmentFactory;

/**
 * TestIndexedStore using ChannelSegment.
 * 
 * @author jwu
 *
 */
public class TestIndexedStoreChannel extends TestIndexedStore {
    @Override
    protected SegmentFactory createStoreSegmentFactory() {
        return new krati.core.segment.ChannelSegmentFactory();
    }
}
