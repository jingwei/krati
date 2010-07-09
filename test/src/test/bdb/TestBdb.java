package test.bdb;

import java.io.File;
import java.io.IOException;

import com.sleepycat.collections.StoredMap;

import test.AbstractSeedTest;
import test.StatsLog;
import test.driver.StoreReader;
import test.driver.StoreTestDriver;
import test.driver.StoreWriter;

public class TestBdb extends AbstractSeedTest
{
    public TestBdb()
    {
        super(TestBdb.class.getSimpleName());
    }
    
    static class BdbReader implements StoreReader<StoredMap<String, String>, String, String>
    {
        @Override
        public final String get(StoredMap<String, String> store, String key)
        {
            return store.get(key);
        }
    }
    
    static class BdbWriter implements StoreWriter<StoredMap<String, String>, String, String>
    {
        @Override
        public final void put(StoredMap<String, String> store, String key, String value)
        {
            store.put(key, value);
        }
    }
    
    @SuppressWarnings("deprecation")
    public void testPerformace() throws IOException
    {
        String unitTestName = getClass().getSimpleName() + ".testPerformance"; 
        StatsLog.beginUnit(unitTestName);
        
        try
        {
            AbstractSeedTest.loadSeedData();
        }
        catch(Exception e)
        {
            e.printStackTrace();
            return;
        }
        
        File dbHome = new File(TEST_OUTPUT_DIR, getClass().getSimpleName());
        if(!dbHome.exists()) dbHome.mkdirs();
        cleanDirectory(dbHome);
        
        SimpleDBEnv dbEnv = new SimpleDBEnv();
        dbEnv.setup(dbHome, false);
        
        StatsLog.logger.info("cacheSize=" + dbEnv.getEnv().getConfig().getCacheSize());
        StatsLog.logger.info("TxnNoSync=" + dbEnv.getEnv().getConfig().getTxnNoSync());
        StatsLog.logger.info("Transactional=" + dbEnv.getEnv().getConfig().getTransactional());
        
        StoredMap<String, String> store = dbEnv.getMap();
        StoreReader<StoredMap<String, String>, String, String> storeReader = new BdbReader();
        StoreWriter<StoredMap<String, String>, String, String> storeWriter = new BdbWriter();
        
        StoreTestDriver<StoredMap<String, String>> driver;
        driver = new StoreTestDriver<StoredMap<String, String>>(store, storeReader, storeWriter, _lineSeedData);
        driver.run(4, 1, runTimeSeconds);
        
        StatsLog.endUnit(unitTestName);
    }
}
