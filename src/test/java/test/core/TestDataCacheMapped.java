package test.core;

import krati.core.segment.SegmentFactory;

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
        return new krati.core.segment.MappedSegmentFactory();
    }
}
