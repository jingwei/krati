package test.set;

import krati.core.segment.ChannelSegmentFactory;
import krati.core.segment.SegmentFactory;

/**
 * TestStaticDataSetChannel
 * 
 * @author jwu
 * 
 */
public class TestStaticDataSetChannel extends TestStaticDataSet {
    @Override
    protected SegmentFactory getSegmentFactory() {
        return new ChannelSegmentFactory();
    }
}
