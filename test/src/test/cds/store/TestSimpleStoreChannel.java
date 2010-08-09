package test.cds.store;

import krati.cds.impl.segment.SegmentFactory;

/**
 * TestSimpleStore using ChannelSegment.
 * 
 * @author jwu
 *
 */
public class TestSimpleStoreChannel extends TestSimpleStore
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.ChannelSegmentFactory();
    }
}
