package test.cds;

import krati.cds.impl.segment.SegmentFactory;

/**
 * TestDataCache using MappedSegment 
 * 
 * @author jwu
 *
 */
public class TestDataCacheMapped extends TestDataCache
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.MappedSegmentFactory();
    }
}
