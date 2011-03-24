package test.core;

import krati.core.segment.SegmentFactory;

/**
 * TestDataPartition using ChannelSegment 
 * 
 * @author jwu
 *
 */
public class TestDataParitionChannel extends TestDataPartition {
    @Override
    protected SegmentFactory getSegmentFactory() {
        return new krati.core.segment.ChannelSegmentFactory();
    }
}
