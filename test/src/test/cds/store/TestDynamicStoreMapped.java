package test.cds.store;

import krati.cds.impl.segment.SegmentFactory;

/**
 * TestDynamicStore using MappedSegment.
 * 
 * @author jwu
 *
 */
public class TestDynamicStoreMapped extends TestDynamicStore
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.MappedSegmentFactory();
    }
}
