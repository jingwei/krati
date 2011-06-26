package test.user;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import test.AbstractTest;
import test.StatsLog;
import test.util.FileUtils;

import junit.framework.TestCase;
import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.SegmentFactory;
import krati.store.DataStore;

/**
 * TestRandomKeyNumStore
 * 
 * @author jwu
 * 06/09, 2011
 */
public class TestRandomKeyNumStore extends TestCase {
    protected File _homeDir;
    protected DataStore<byte[], byte[]> _store;
    protected Random _rand = new Random();
    
    @Override
    protected void setUp() {
        try {
            _homeDir = FileUtils.getTestDir(getClass().getSimpleName());
            _store = createStore(_homeDir);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            _store.close();
            FileUtils.deleteDirectory(_homeDir);
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            _homeDir = null;
            _store = null;
        }
    }
    
    protected int getKeySize() {
        return 10;
    }
    
    protected int getValueSize() {
        return 4;
    }
    
    protected int getKeyCount() {
        return AbstractTest._keyCount;
    }
    
    protected int getSegmentFileSizeMB() {
        return AbstractTest._segFileSizeMB;
    }
    
    protected SegmentFactory createSegmentFactory() {
        return new MappedSegmentFactory();
    }
    
    protected int getCapacity() {
        return (int)Math.min((long)(getKeyCount() * 1.5), Integer.MAX_VALUE);
    }
    
    protected DataStore<byte[], byte[]> createStore(File homeDir) throws Exception {
        StoreConfig config = new StoreConfig(homeDir, getCapacity());
        config.setSegmentFileSizeMB(getSegmentFileSizeMB());
        config.setSegmentFactory(createSegmentFactory());
        
        return StoreFactory.createDynamicDataStore(config);
    }
    
    public void test() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        long startTime = System.currentTimeMillis();
        
        byte[] key = new byte[getKeySize()];
        byte[] value = new byte[getValueSize()];
        
        Map<String, byte[]> map = new HashMap<String, byte[]>();
        float threshold = 1000.0F / getKeyCount();
        
        for(int i = 0, cnt = getKeyCount(); i < cnt; i++) {
            _rand.nextBytes(key);
            _rand.nextBytes(value);
            _store.put(key, value);
            
            if(_rand.nextFloat() < threshold) {
                map.put(new String((byte[])key.clone()), (byte[])value.clone());
            }
        }
        
        _store.sync();
        
        for(Map.Entry<String, byte[]> e : map.entrySet()) {
            assertTrue(Arrays.equals(e.getValue(), _store.get(e.getKey().getBytes())));
        }
        StatsLog.logger.info(map.size() + " keys verfied");
        
        long elapsedTime = System.currentTimeMillis() - startTime;
        double rate = Math.ceil(getKeyCount() * 100.0 / elapsedTime) / 100.0;
        StatsLog.logger.info("#ops=" + getKeyCount() + " time=" + elapsedTime + " ms rate=" + rate + " per ms");
        StatsLog.endUnit(unitTestName);
    }
}
