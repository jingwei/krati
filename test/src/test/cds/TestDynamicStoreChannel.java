package test.cds;

import krati.cds.impl.segment.SegmentFactory;

/**
 * TestDynamicStore using ChannleSegment.
 * 
 * @author jwu
 *
 */
public class TestDynamicStoreChannel extends TestDynamicStore
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.ChannelSegmentFactory();
    }
    
    @Override
    public void testDynamicStore() throws Exception
    {
        new TestDynamicStoreChannel().run(4, 1);
        System.out.println("done");
        cleanTestOutput();
    }
}
