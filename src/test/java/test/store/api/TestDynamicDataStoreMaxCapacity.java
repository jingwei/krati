package test.store.api;

import java.io.File;

import test.util.FileUtils;

import junit.framework.TestCase;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.array.basic.DynamicConstants;
import krati.store.DynamicDataStore;
import krati.util.LinearHashing;

/**
 * TestDynamicDataStoreMaxCapacity
 * 
 * @author jwu
 * 06/26, 2011
 */
public class TestDynamicDataStoreMaxCapacity extends TestCase {

    protected File getHomeDir() {
        return FileUtils.getTestDir(getClass().getSimpleName());
    }
    
    @Override
    protected void tearDown() {
        try {
            FileUtils.deleteDirectory(getHomeDir());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    public void testMaxCapacity() throws Exception {
        StoreConfig config = new StoreConfig(getHomeDir(), Integer.MAX_VALUE);
        config.setIndexesCached(false); // Do not cache indexes in memory
        
        DynamicDataStore store = StoreFactory.createDynamicDataStore(config);
        
        // Compute maxLevel
        LinearHashing h = new LinearHashing(DynamicConstants.SUB_ARRAY_SIZE);
        h.reinit(Integer.MAX_VALUE);
        int maxLevel = h.getLevel();
        
        // Check store initLevel
        assertEquals(maxLevel, store.getLevel());
        
        // Check store capacity
        int capacityExpected = DynamicConstants.SUB_ARRAY_SIZE << maxLevel; 
        assertEquals(capacityExpected, store.capacity());
        assertEquals(capacityExpected, store.getDataArray().length());
        
        store.close();
    }
}
