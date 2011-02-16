package test.set;

import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.SegmentFactory;

public class TestStaticDataSetMapped extends TestStaticDataSet
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new MappedSegmentFactory();
    }
}
