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

package test.store;

import java.io.File;
import java.util.Iterator;

import krati.store.DataStore;
import test.AbstractTest;
import test.StatsLog;
import test.driver.StoreReader;
import test.driver.StoreTestDriver;
import test.driver.StoreWriter;
import test.driver.raw.StoreTestBytesDriver;

/**
 * EvalDataStore
 * 
 * @author jwu
 */
public abstract class EvalDataStore extends AbstractTest {
    
    public EvalDataStore(String name) {
        super(name);
    }
    
    protected abstract DataStore<byte[], byte[]> getDataStore(File DataStoreDir) throws Exception;
    
    static class DataStoreReader implements StoreReader<DataStore<byte[], byte[]>, byte[], byte[]> {
        @Override
        public final byte[] get(DataStore<byte[], byte[]> store, byte[] key) {
            return (key == null) ? null : store.get(key);
        }
    }
    
    static class DataStoreWriter implements StoreWriter<DataStore<byte[], byte[]>, byte[], byte[]> {
        @Override
        public final void put(DataStore<byte[], byte[]> store, byte[] key, byte[] value) throws Exception {
            store.put(key, value);
        }
    }
    
    public void evalPerformance(int numOfReaders, int numOfWriters, int runDuration) throws Exception {
        File storeDir = getHomeDirectory();
        if(!storeDir.exists()) storeDir.mkdirs();
        cleanDirectory(storeDir);
        
        DataStore<byte[], byte[]> store = getDataStore(storeDir);
        StoreReader<DataStore<byte[], byte[]>, byte[], byte[]> storeReader = new DataStoreReader();
        StoreWriter<DataStore<byte[], byte[]>, byte[], byte[]> storeWriter = new DataStoreWriter();
        
        StoreTestDriver driver;
        driver = new StoreTestBytesDriver<DataStore<byte[], byte[]>>(store, storeReader, storeWriter, _lineSeedData, _keyCount, _hitPercent);
        driver.run(numOfReaders, numOfWriters, runDuration);
        
        store.sync();
        
        try {
            iterate(store, 100);
        } catch(UnsupportedOperationException e) {}
    }
    
    protected void iterate(DataStore<byte[], byte[]> store, int runTimeSeconds) {
        int cnt = 0;
        long total = runTimeSeconds * 1000;
        long start = System.currentTimeMillis();
        Iterator<byte[]> iter = store.keyIterator();
        StatsLog.logger.info(">>> iterate");
        
        byte[] key = null;
        while (iter.hasNext()) {
            key = iter.next();
            store.get(key);
            cnt++;
            
            if((System.currentTimeMillis() - start) > total) break;
        }
        
        StatsLog.logger.info("read " + cnt + " key-value(s) in " + (System.currentTimeMillis() - start) + " ms");
    }
}
