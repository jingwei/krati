package test.cds.store;

import krati.cds.impl.segment.SegmentFactory;

/**
 * TestSimpleStore using MappedSegment.
 * 
 * @author jwu
 *
 */
public class TestSimpleStoreMapped extends TestSimpleStore
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.MappedSegmentFactory();
    }
}
