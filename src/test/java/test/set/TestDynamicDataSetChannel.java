package test.set;

import krati.core.segment.ChannelSegmentFactory;
import krati.core.segment.SegmentFactory;

/**
 * TestDynamicDataSetChannel
 * 
 * @author jwu
 * 
 */
public class TestDynamicDataSetChannel extends TestDynamicDataSet {
    @Override
    protected SegmentFactory getSegmentFactory() {
        return new ChannelSegmentFactory();
    }
}
