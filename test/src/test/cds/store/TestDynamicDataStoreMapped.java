package test.cds.store;

import krati.cds.impl.segment.SegmentFactory;

/**
 * TestDynamicDataStore using MappedSegment.
 * 
 * @author jwu
 *
 */
public class TestDynamicDataStoreMapped extends TestDynamicDataStore
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.MappedSegmentFactory();
    }
}
