package test.cds;

import krati.cds.impl.segment.SegmentFactory;

/**
 * TestSimpleStore using ChannelSegment.
 * 
 * @author jwu
 *
 */
public class TestSimpleStoreChannel extends TestSimpleStore
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.ChannelSegmentFactory();
    }
    
    @Override
    public void testSimpleStore() throws Exception
    {
        new TestSimpleStoreChannel().run(4, 1);
        System.out.println("done");
        cleanTestOutput();
    }
}
