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

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.StoreParams;
import krati.store.DataStore;
import krati.store.index.HashIndexDataHandler;
import test.StatsLog;

/**
 * TestIndexedStoreSmallIndex
 * 
 * @author jwu
 * @since 08/15, 2012
 */
public class TestIndexedStoreSmallIndex extends EvalDataStore {

    public TestIndexedStoreSmallIndex() {
        super(TestIndexedStore.class.getSimpleName());
    }
    
    public int getInitialCapacity() {
        return _keyCount / 8;
    }
    
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception {
        StoreConfig config = new StoreConfig(storeDir, getInitialCapacity());
        config.setBatchSize(10000);
        config.setNumSyncBatches(100);
        config.setSegmentFactory(new krati.core.segment.WriteBufferSegmentFactory());
        config.setSegmentFileSizeMB(_segFileSizeMB);
        config.setSegmentCompactFactor(0.67);
        
        // Disable linear hashing
        config.setHashLoadFactor(1.0);
        
        // Configure index segments
        config.setInt(StoreParams.PARAM_INDEX_SEGMENT_FILE_SIZE_MB, 32);
        config.setDouble(StoreParams.PARAM_INDEX_SEGMENT_COMPACT_FACTOR, 0.5);
        
        // Configure for small memory footprint
        config.setDataHandler(new HashIndexDataHandler());
        
        return StoreFactory.createIndexedDataStore(config);
    }
    
    public void testIndexedStore() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        evalPerformance(_numReaders, 1, _runTimeSeconds);
        cleanTestOutput();
        
        StatsLog.endUnit(unitTestName);
    }
}
