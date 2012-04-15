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

package test.set.api;

import java.io.File;
import java.util.Random;

import test.util.FileUtils;
import test.util.RandomBytes;
import junit.framework.TestCase;
import krati.core.StoreFactory;
import krati.core.StoreParams;
import krati.core.array.basic.DynamicConstants;
import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.SegmentFactory;
import krati.store.DataSet;

/**
 * TestSetFactory
 * 
 * @author jwu
 * 06/11, 2011
 * 
 */
public class TestSetFactory extends TestCase {
    Random _rand = new Random();
    
    public void testCreateStaticDataSet() throws Exception {
        File homeDir = FileUtils.getTestDir(getClass().getSimpleName());
        int capacity = 1000 + _rand.nextInt(1000000);
        int batchSize = StoreParams.BATCH_SIZE_DEFAULT;
        int numSyncBatches = StoreParams.NUM_SYNC_BATCHES_DEFAULT;
        int segmentFileSizeMB = StoreParams.SEGMENT_FILE_SIZE_MB_MIN;
        double segmentCompactFactor = StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT;
        SegmentFactory segmentFactory = new MappedSegmentFactory();

        DataSet<byte[]> store;
        byte[] key = RandomBytes.getBytes(32);
        
        store = StoreFactory.createStaticDataSet(
                homeDir,
                capacity,
                segmentFileSizeMB,
                segmentFactory);
        
        store.add(key);
        assertTrue(store.has(key));
        store.close();
        
        store = StoreFactory.createStaticDataSet(
                homeDir,
                capacity,
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory);
        
        assertTrue(store.has(key));
        store.close();
        
        store = StoreFactory.createStaticDataSet(
                homeDir,
                capacity,
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactFactor);
        
        assertTrue(store.has(key));
        store.close();
        
        FileUtils.deleteDirectory(homeDir);
    }
    
    public void testCreateDynamicDataSet() throws Exception {
        File homeDir = FileUtils.getTestDir(getClass().getSimpleName());
        int capacity = DynamicConstants.SUB_ARRAY_SIZE << 2;
        int batchSize = StoreParams.BATCH_SIZE_DEFAULT;
        int numSyncBatches = StoreParams.NUM_SYNC_BATCHES_DEFAULT;
        int segmentFileSizeMB = StoreParams.SEGMENT_FILE_SIZE_MB_MIN;
        double segmentCompactFactor = StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT;
        double hashLoadFactor = StoreParams.HASH_LOAD_FACTOR_DEFAULT;
        SegmentFactory segmentFactory = new MappedSegmentFactory();
        
        DataSet<byte[]> store;
        byte[] key = RandomBytes.getBytes(32);
        
        store = StoreFactory.createDynamicDataSet(
                homeDir,
                capacity,
                segmentFileSizeMB,
                segmentFactory);
        
        store.add(key);
        assertTrue(store.has(key));
        store.close();
        
        store = StoreFactory.createDynamicDataSet(
                homeDir,
                capacity,
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory);

        assertTrue(store.has(key));
        store.close();
        
        store = StoreFactory.createDynamicDataSet(
                homeDir,
                capacity,
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactFactor);
        
        assertTrue(store.has(key));
        store.close();
        
        store = StoreFactory.createDynamicDataSet(
                homeDir,
                capacity,
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactFactor,
                hashLoadFactor);
        
        assertTrue(store.has(key));
        store.close();
        
        FileUtils.deleteDirectory(homeDir);
    }
}
