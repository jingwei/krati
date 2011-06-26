package test.store.api;

import java.io.File;

import test.util.FileUtils;

import junit.framework.TestCase;
import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.store.StaticDataStore;

/**
 * TestStaticDataStoreMaxCapacity
 * 
 * @author jwu
 * 06/26, 2011
 */
public class TestStaticDataStoreMaxCapacity extends TestCase {
    
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
        
        StaticDataStore store = StoreFactory.createStaticDataStore(config);
        
        // Check store capacity
        assertEquals(Integer.MAX_VALUE, store.capacity());
        assertEquals(Integer.MAX_VALUE, store.getDataArray().length());
        
        store.close();
    }
}
