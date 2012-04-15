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
