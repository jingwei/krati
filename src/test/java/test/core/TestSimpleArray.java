package test.core;

import java.io.File;
import java.io.IOException;

import krati.core.array.AddressArray;
import krati.core.array.SimpleDataArray;
import krati.core.array.basic.StaticLongArray;
import krati.core.segment.SegmentFactory;
import krati.core.segment.SegmentManager;
import test.AbstractSeedTest;
import test.StatsLog;

/**
 * Test SimpleDataArray.
 * 
 * @author jwu
 *
 */
public class TestSimpleArray extends AbstractSeedTest {
    
    public TestSimpleArray() {
        super(TestSimpleArray.class.getSimpleName());
    }
    
    protected SegmentFactory getSegmentFactory() {
        return new krati.core.segment.ChannelSegmentFactory();
    }
    
    protected AddressArray getAddressArray(File homeDir) throws Exception {
        return new StaticLongArray(_idCount, 10000, 5, homeDir);
    }
    
    protected SegmentManager getSegmentManager(File homeDir) throws IOException {
        String segmentHome = homeDir.getCanonicalPath() + File.separator + "segs";
        return SegmentManager.getInstance(segmentHome, getSegmentFactory(), _segFileSizeMB);
    }
    
    protected SimpleDataArray getDataArray(File homeDir) throws Exception {
        SimpleDataArray array = new SimpleDataArray(getAddressArray(homeDir),
                                                    getSegmentManager(homeDir));
        return array;
    }
    
    public void populate(SimpleDataArray dataArray) throws IOException {
        String line;
        int index = 0;
        int length = dataArray.length();
        int lineCnt = _lineSeedData.size();
        
        long scn = dataArray.getHWMark();
        long startTime = System.currentTimeMillis();
        
        while (index < length) {
            try {
                line = _lineSeedData.get(index % lineCnt);
                dataArray.set(index, line.getBytes(), scn++);
            } catch (Exception e) {
                e.printStackTrace();
            }
            index++;
        }
        
        dataArray.persist();

        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        
        double rate = dataArray.length()/(double)elapsedTime;
        rate = Math.round(rate * 100) / 100.0;
        StatsLog.logger.info("writeCount=" + dataArray.length() + " rate=" + rate + " per ms");
        StatsLog.logger.info("elapsedTime=" + elapsedTime + " ms");
    }
    
    public void testPopulate() {
        String unitTestName = getClass().getSimpleName() + " with " + getSegmentFactory().getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        try {
            AbstractSeedTest.loadSeedData();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        
        try {
            SimpleDataArray array;
            
            File homeDir = getHomeDirectory();
            array = getDataArray(homeDir);
            
            StatsLog.logger.info(">>> populate");
            populate(array);
            
            array.sync();
        } catch (Exception e) {
            StatsLog.logger.info(e.getMessage(), e);
            e.printStackTrace();
        }
        
        StatsLog.endUnit(unitTestName);
    }
}
