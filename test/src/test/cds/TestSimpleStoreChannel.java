package test.cds;

import test.StatsLog;
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
        String unitTestName = getClass().getSimpleName() + " with " + getSegmentFactory().getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        new TestSimpleStoreChannel().evalPerformance(4, 1, _runTimeSeconds);
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
