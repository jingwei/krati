package test.core.api;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import krati.core.array.AddressArray;
import krati.core.array.SimpleDataArray;
import krati.core.array.basic.StaticLongArray;
import krati.core.segment.SegmentFactory;
import krati.core.segment.SegmentManager;
import test.AbstractTest;
import test.StatsLog;
import test.util.FileUtils;

/**
 * Test SimpleDataArray.
 * 
 * @author jwu
 * 
 * <p>
 * 05/15, 2011 - Created
 * 05/22, 2011 - Added test for open/close
 * 05/24, 2011 - Added test for clear
 * 05/24, 2011 - Added test for partial reads
 */
public class TestSimpleDataArray extends AbstractTest {
    protected final Random _rand = new Random(); 
    protected SimpleDataArray _dataArray;
    
    public TestSimpleDataArray() {
        super(TestSimpleDataArray.class.getSimpleName());
    }
    
    protected SegmentFactory createSegmentFactory() {
        return new krati.core.segment.ChannelSegmentFactory();
    }
    
    protected AddressArray createAddressArray(File homeDir) throws Exception {
        return new StaticLongArray(_idCount, 1000, 5, homeDir);
    }
    
    protected SegmentManager createSegmentManager(File homeDir) throws IOException {
        String segmentHome = homeDir.getCanonicalPath() + File.separator + "segs";
        return SegmentManager.getInstance(segmentHome, createSegmentFactory(), _segFileSizeMB);
    }
    
    protected SimpleDataArray createDataArray(File homeDir) throws Exception {
        SimpleDataArray array = new SimpleDataArray(createAddressArray(homeDir),
                                                    createSegmentManager(homeDir));
        return array;
    }
    
    protected void setUp() {
        try {
            _dataArray = createDataArray(getHomeDirectory());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    protected void tearDown() {
        try {
            _dataArray.close();
            FileUtils.deleteDirectory(getHomeDirectory());
        } catch(Exception e) {
            e.printStackTrace();
        }
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
    
    public void check(SimpleDataArray dataArray) throws IOException {
        String line;
        int index = 0;
        int length = dataArray.length();
        int lineCnt = _lineSeedData.size();
        
        long startTime = System.currentTimeMillis();
        
        while (index < length) {
            try {
                line = _lineSeedData.get(index % lineCnt);
                String lineRead = new String(dataArray.get(index));
                assertEquals(line, lineRead);
            } catch (Exception e) {
                e.printStackTrace();
            }
            index++;
        }
        
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        
        double rate = dataArray.length()/(double)elapsedTime;
        rate = Math.round(rate * 100) / 100.0;
        StatsLog.logger.info("checkCount=" + dataArray.length() + " rate=" + rate + " per ms");
        StatsLog.logger.info("elapsedTime=" + elapsedTime + " ms");
    }
    
    public void testOpenClose() {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        try {
            StatsLog.logger.info(">>> populate");
            populate(_dataArray);
            _dataArray.sync();
            
            long hwmScn = _dataArray.getHWMark();
            
            // Test open/close (1st reopen)
            _dataArray.close();
            _dataArray.open();
            
            assertEquals(hwmScn, _dataArray.getLWMark());
            assertEquals(hwmScn, _dataArray.getHWMark());
            
            StatsLog.logger.info(">>> check after 1st reopen");
            check(_dataArray);
            
            // Test open/close (2nd reopen)
            _dataArray.close();
            _dataArray.open();
            
            assertEquals(hwmScn, _dataArray.getLWMark());
            assertEquals(hwmScn, _dataArray.getHWMark());
            
            StatsLog.logger.info(">>> check after 2nd reopen");
            check(_dataArray);
            
            _dataArray.close();
        } catch (Exception e) {
            StatsLog.logger.info(e.getMessage(), e);
            e.printStackTrace();
        }
        
        StatsLog.endUnit(unitTestName);
    }
    
    public void testClear() {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        try {
            StatsLog.logger.info(">>> populate");
            populate(_dataArray);
            _dataArray.sync();
            
            StatsLog.logger.info(">>> check");
            check(_dataArray);
            
            // Clear data array
            _dataArray.clear();
            
            // Check that array is cleared
            for(int i = 0, cnt = _dataArray.length(); i < cnt; i++) {
                assertEquals(null, _dataArray.get(i));
            }
            
            // Reopen data array
            _dataArray.close();
            _dataArray.open();
            
            // Check that array is cleared
            for(int i = 0, cnt = _dataArray.length(); i < cnt; i++) {
                assertEquals(null, _dataArray.get(i));
            }
            
            _dataArray.close();
        } catch (Exception e) {
            StatsLog.logger.info(e.getMessage(), e);
            e.printStackTrace();
        }
        
        StatsLog.endUnit(unitTestName);
    }
    
    public void testPartialRead() throws Exception {
        for(int i = 0, cnt = _rand.nextInt(100); i < cnt; i++) {
            int index = _rand.nextInt(_dataArray.length());
            String line = _lineSeedData.get(index%_lineSeedData.size());
            byte[] data = line.getBytes();
            
            _dataArray.set(index, data, System.currentTimeMillis());
            
            byte[] src = new byte[data.length/2];
            byte[] dst = new byte[data.length/2];
            
            System.arraycopy(data, 0, src, 0, dst.length);
            assertEquals(dst.length, _dataArray.read(index, dst));
            assertTrue(Arrays.equals(src, dst));
            
            System.arraycopy(data, dst.length, src, 0, dst.length);
            assertEquals(dst.length, _dataArray.read(index, dst.length, dst));
            assertTrue(Arrays.equals(src, dst));
        }
    }
}
