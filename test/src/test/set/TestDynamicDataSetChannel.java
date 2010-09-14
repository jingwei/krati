package test.set;

import krati.core.segment.ChannelSegmentFactory;
import krati.core.segment.SegmentFactory;

public class TestDynamicDataSetChannel extends TestDynamicDataSet
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new ChannelSegmentFactory();
    }
}
