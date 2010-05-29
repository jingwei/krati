package test.cds;

import krati.cds.impl.segment.SegmentFactory;

/**
 * TestSimpleStore using MappedSegment.
 * 
 * @author jwu
 *
 */
public class TestSimpleStoreMapped extends TestSimpleStore
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.MappedSegmentFactory();
    }
    
    @Override
    public void testSimpleStore() throws Exception
    {
        new TestSimpleStoreMapped().run(4, 1);
        System.out.println("done");
        cleanTestOutput();
    }
}
