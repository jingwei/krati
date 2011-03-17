package test.store;

import krati.core.segment.SegmentFactory;

/**
 * TestStaticStore using ChannelSegment.
 * 
 * @author jwu
 *
 */
public class TestStaticStoreChannel extends TestStaticStore {
    @Override
    protected SegmentFactory getSegmentFactory() {
        return new krati.core.segment.ChannelSegmentFactory();
    }
}
