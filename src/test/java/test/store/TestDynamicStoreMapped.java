package test.store;

import krati.core.segment.SegmentFactory;

/**
 * TestDynamicStore using MappedSegment.
 * 
 * @author jwu
 *
 */
public class TestDynamicStoreMapped extends TestDynamicStore {
    @Override
    protected SegmentFactory getSegmentFactory() {
        return new krati.core.segment.MappedSegmentFactory();
    }
}
