package test.store;

import krati.core.segment.SegmentFactory;

/**
 * TestStaticStore using MappedSegment.
 * 
 * @author jwu
 *
 */
public class TestStaticStoreMapped extends TestStaticStore {
    @Override
    protected SegmentFactory getSegmentFactory() {
        return new krati.core.segment.MappedSegmentFactory();
    }
}
