package test.store;

import krati.core.segment.SegmentFactory;

/**
 * TestDynamicStore using ChannleSegment.
 * 
 * @author jwu
 *
 */
public class TestDynamicStoreChannel extends TestDynamicStore {
    @Override
    protected SegmentFactory getSegmentFactory() {
        return new krati.core.segment.ChannelSegmentFactory();
    }
}
