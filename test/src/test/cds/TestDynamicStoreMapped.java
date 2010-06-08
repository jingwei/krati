package test.cds;

import krati.cds.impl.segment.SegmentFactory;

/**
 * TestDynamicStore using MappedSegment.
 * 
 * @author jwu
 *
 */
public class TestDynamicStoreMapped extends TestDynamicStore
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.MappedSegmentFactory();
    }
    
    @Override
    public void testDynamicStore() throws Exception
    {
        new TestDynamicStoreMapped().run(4, 1);
        System.out.println("done");
        cleanTestOutput();
    }
}
