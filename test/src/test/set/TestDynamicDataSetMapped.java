package test.set;

import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.SegmentFactory;

public class TestDynamicDataSetMapped extends TestDynamicDataSet
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new MappedSegmentFactory();
    }
}
