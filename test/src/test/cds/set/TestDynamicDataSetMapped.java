package test.cds.set;

import krati.cds.impl.segment.MappedSegmentFactory;
import krati.cds.impl.segment.SegmentFactory;

public class TestDynamicDataSetMapped extends TestDynamicDataSet
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new MappedSegmentFactory();
    }
}
