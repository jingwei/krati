package test.core;

import krati.core.segment.SegmentFactory;

/**
 * TestDataCache using ChannelSegment 
 * 
 * @author jwu
 *
 */
public class TestDataCacheChannel extends TestDataCache {
    @Override
    protected SegmentFactory getSegmentFactory() {
        return new krati.core.segment.ChannelSegmentFactory();
    }
}
