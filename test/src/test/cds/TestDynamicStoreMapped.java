package test.cds;

import test.StatsLog;
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
        String unitTestName = getClass().getSimpleName() + " with " + getSegmentFactory().getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        new TestDynamicStoreMapped().evalPerformance(4, 1, _runTimeSeconds);
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
