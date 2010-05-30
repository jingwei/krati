package test.cds;

import krati.cds.impl.segment.SegmentFactory;

/**
 * TestDataCache using ChannelSegment 
 * 
 * @author jwu
 *
 */
public class TestDataCacheChannel extends TestDataCache
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.ChannelSegmentFactory();
    }
}
