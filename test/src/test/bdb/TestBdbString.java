package test.bdb;

import java.io.File;
import java.io.IOException;

import com.sleepycat.collections.StoredMap;

import test.AbstractSeedTest;
import test.StatsLog;
import test.driver.StoreReader;
import test.driver.StoreTestDriver;
import test.driver.StoreWriter;
import test.driver.string.StoreTestStringDriver;

public class TestBdbString extends AbstractSeedTest
{
    public TestBdbString()
    {
        super(TestBdbString.class.getSimpleName());
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
        
        File storeDir = new File(TEST_OUTPUT_DIR, getClass().getSimpleName());
        if(!storeDir.exists()) storeDir.mkdirs();
        cleanDirectory(storeDir);
        
        SimpleDBEnv dbEnv = new SimpleDBEnv();
        dbEnv.setup(storeDir, false);
        
        StatsLog.logger.info("cacheSize=" + dbEnv.getEnv().getConfig().getCacheSize());
        StatsLog.logger.info("TxnNoSync=" + dbEnv.getEnv().getConfig().getTxnNoSync());
        StatsLog.logger.info("Transactional=" + dbEnv.getEnv().getConfig().getTransactional());
        
        StoredMap<String, String> store = dbEnv.getMap();
        StoreReader<StoredMap<String, String>, String, String> storeReader = new BdbReader();
        StoreWriter<StoredMap<String, String>, String, String> storeWriter = new BdbWriter();
        
        StoreTestDriver driver;
        driver = new StoreTestStringDriver<StoredMap<String, String>>(store, storeReader, storeWriter, _lineSeedData, _keyCount);
        driver.run(4, 1, _runTimeSeconds);
        
        StatsLog.endUnit(unitTestName);
    }
}
