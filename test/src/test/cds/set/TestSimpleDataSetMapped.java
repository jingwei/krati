package test.cds.set;

import krati.cds.impl.segment.MappedSegmentFactory;
import krati.cds.impl.segment.SegmentFactory;

public class TestSimpleDataSetMapped extends TestSimpleDataSet
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new MappedSegmentFactory();
    }
}
