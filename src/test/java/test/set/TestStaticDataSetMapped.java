package test.set;

import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.SegmentFactory;

/**
 * TestStaticDataSetMapped
 * 
 * @author jwu
 * 
 */
public class TestStaticDataSetMapped extends TestStaticDataSet {
    @Override
    protected SegmentFactory getSegmentFactory() {
        return new MappedSegmentFactory();
    }
}
