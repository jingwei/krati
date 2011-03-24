package test.core;

import krati.core.segment.SegmentFactory;

/**
 * TestDataPartition using MappedSegment 
 * 
 * @author jwu
 *
 */
public class TestDataPartitionMapped extends TestDataPartition {
    @Override
    protected SegmentFactory getSegmentFactory() {
       return new krati.core.segment.MappedSegmentFactory();
    }
}
