package test.cds;

import krati.cds.impl.segment.SegmentFactory;

/**
 * TestDynamicDataStore using ChannelSegment.
 * 
 * @author jwu
 *
 */
public class TestDynamicDataStoreChannel extends TestDynamicDataStore
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.ChannelSegmentFactory();
    }
}
