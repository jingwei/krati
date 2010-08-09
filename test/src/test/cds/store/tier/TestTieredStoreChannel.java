package test.cds.store.tier;

import krati.cds.impl.segment.SegmentFactory;

public class TestTieredStoreChannel extends EvalTieredStore
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.ChannelSegmentFactory();
    }
}
