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

package test.set;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;
import krati.core.StoreConfig;
import krati.core.array.basic.DynamicConstants;
import krati.core.segment.ChannelSegmentFactory;
import krati.store.DynamicDataSet;
import test.util.DirUtils;

/**
 * TestDynamicDataSetCapacity
 * 
 * @author jwu
 * @since 09/05, 2012
 */
public class TestDynamicDataSetCapacity extends TestCase {
    protected Random _rand = new Random();
    
    protected DynamicDataSet create(int initialCapacity) throws Exception {
        File storeDir = DirUtils.getTestDir(getClass());
        StoreConfig config = new StoreConfig(storeDir, initialCapacity);
        config.setSegmentFactory(new ChannelSegmentFactory());
        config.setSegmentFileSizeMB(16);
        return new DynamicDataSet(config);
    }
    
    @Override
    protected void tearDown() {
        File storeDir = DirUtils.getTestDir(getClass());
        try {
            DirUtils.deleteDirectory(storeDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    protected void doInitialCapacity(int initialCapacity) throws Exception {
        DynamicDataSet store = create(initialCapacity);
        int capacity = store.capacity();
        if(initialCapacity == DynamicConstants.SUB_ARRAY_SIZE) {
            assertEquals(initialCapacity, capacity);
        } else {
            assertTrue(capacity >= initialCapacity);
        }
        store.close();

        int initialCapacity2 = capacity + _rand.nextInt(capacity);
        DynamicDataSet store2 = create(initialCapacity2);
        assertEquals(capacity, store2.capacity());
        store2.close();

        int initialCapacity3 = capacity - _rand.nextInt(capacity);
        DynamicDataSet store3 = create(initialCapacity3);
        assertEquals(capacity, store3.capacity());
        store3.close();
    }
    
    public void testInitialCapacity1() throws Exception {
        doInitialCapacity(DynamicConstants.SUB_ARRAY_SIZE);
    }
    
    public void testInitialCapacity2() throws Exception {
        doInitialCapacity(DynamicConstants.SUB_ARRAY_SIZE + _rand.nextInt(1000000));
    }
    
    public void testInitialCapacity3() throws Exception {
        doInitialCapacity(DynamicConstants.SUB_ARRAY_SIZE - _rand.nextInt(10000));
    }
    
    protected void doInitialCapacityLevel(int initLevel) throws Exception {
        int initialCapacity = DynamicConstants.SUB_ARRAY_SIZE << initLevel;
        DynamicDataSet store = create(initialCapacity);
        int capacity = store.capacity();
        assertEquals(initialCapacity, capacity);
        store.close();
        
        int initialCapacity2 = capacity + _rand.nextInt(capacity);
        DynamicDataSet store2 = create(initialCapacity2);
        assertEquals(capacity, store2.capacity());
        store2.close();
        
        int initialCapacity3 = capacity - _rand.nextInt(capacity);
        DynamicDataSet store3 = create(initialCapacity3);
        assertEquals(capacity, store3.capacity());
        store3.close();
    }
    
    public void testInitialCapacityLevel0() throws Exception {
        doInitialCapacityLevel(0);
    }
    
    public void testInitialCapacityLevelX() throws Exception {
        doInitialCapacityLevel(_rand.nextInt(10));
    }
}
