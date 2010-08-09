package test.cds.store;

import krati.cds.impl.segment.SegmentFactory;

/**
 * TestDynamicStore using ChannleSegment.
 * 
 * @author jwu
 *
 */
public class TestDynamicStoreChannel extends TestDynamicStore
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.ChannelSegmentFactory();
    }
}
