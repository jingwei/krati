package test.bdb;

import java.io.File;
import java.io.IOException;

import com.sleepycat.je.Database;

import test.AbstractTest;
import test.StatsLog;
import test.driver.StoreReader;
import test.driver.StoreTestDriver;
import test.driver.StoreWriter;
import test.driver.raw.StoreTestBytesDriver;

/**
 * TestBdbBytes
 * 
 * @author jwu
 * 
 */
public class TestBdbBytes extends AbstractTest {
    public TestBdbBytes() {
        super(TestBdbString.class.getSimpleName());
    }

    @SuppressWarnings("deprecation")
    public void testPerformace() throws IOException {
        String unitTestName = getClass().getSimpleName() + ".testPerformance";
        StatsLog.beginUnit(unitTestName);

        File storeDir = getHomeDirectory();
        if (!storeDir.exists())
            storeDir.mkdirs();
        cleanDirectory(storeDir);

        SimpleDBEnv dbEnv = new SimpleDBEnv();
        dbEnv.setup(storeDir, false);

        StatsLog.logger.info("cacheSize=" + dbEnv.getEnv().getConfig().getCacheSize());
        StatsLog.logger.info("TxnNoSync=" + dbEnv.getEnv().getConfig().getTxnNoSync());
        StatsLog.logger.info("Transactional=" + dbEnv.getEnv().getConfig().getTransactional());

        Database store = dbEnv.getSimpleDB();
        StoreReader<Database, byte[], byte[]> storeReader = new BdbBytesReader();
        StoreWriter<Database, byte[], byte[]> storeWriter = new BdbBytesWriter();

        StoreTestDriver driver;
        driver = new StoreTestBytesDriver<Database>(store, storeReader, storeWriter, _lineSeedData, _keyCount, _hitPercent);
        driver.run(_numReaders, 1, _runTimeSeconds);

        StatsLog.endUnit(unitTestName);
    }
}
