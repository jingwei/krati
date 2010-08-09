package test.cds.set;

import krati.cds.impl.segment.ChannelSegmentFactory;
import krati.cds.impl.segment.SegmentFactory;

public class TestDynamicDataSetChannel extends TestDynamicDataSet
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new ChannelSegmentFactory();
    }
}
