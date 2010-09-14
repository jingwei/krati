package test.store;

import krati.core.segment.SegmentFactory;

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
        return new krati.core.segment.MappedSegmentFactory();
    }
}
