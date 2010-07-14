package test.cds;

import test.StatsLog;
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
        String unitTestName = getClass().getSimpleName() + " with " + getSegmentFactory().getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        new TestSimpleStoreMapped().evalPerformance(4, 1, _runTimeSeconds);
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
