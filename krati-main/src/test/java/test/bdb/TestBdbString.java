/*
 * Copyright (c) 2010-2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package test.bdb;

import java.io.File;
import java.io.IOException;

import com.sleepycat.collections.StoredMap;

import test.AbstractTest;
import test.StatsLog;
import test.driver.StoreReader;
import test.driver.StoreTestDriver;
import test.driver.StoreWriter;
import test.driver.string.StoreTestStringDriver;

/**
 * TestBdbString
 * 
 * @author jwu
 * 
 */
public class TestBdbString extends AbstractTest {
    public TestBdbString() {
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

        StoredMap<String, String> store = dbEnv.getMap();
        StoreReader<StoredMap<String, String>, String, String> storeReader = new BdbStringReader();
        StoreWriter<StoredMap<String, String>, String, String> storeWriter = new BdbStringWriter();

        StoreTestDriver driver;
        driver = new StoreTestStringDriver<StoredMap<String, String>>(store, storeReader, storeWriter, _lineSeedData, _keyCount, _hitPercent);
        driver.run(_numReaders, 1, _runTimeSeconds);

        StatsLog.endUnit(unitTestName);
    }
}
