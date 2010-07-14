package test.cds;

import test.StatsLog;
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
        String unitTestName = getClass().getSimpleName() + " with " + getSegmentFactory().getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        new TestDynamicStoreChannel().evalPerformance(4, 1, _runTimeSeconds);
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
