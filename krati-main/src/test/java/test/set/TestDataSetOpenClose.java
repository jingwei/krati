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
import java.util.Iterator;
import java.util.Random;

import test.util.DirUtils;

import junit.framework.TestCase;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.segment.MemorySegmentFactory;
import krati.store.DataSet;

/**
 * TestDataSetOpenClose
 * 
 * @author jwu
 * @since 06/12, 2012
 */
public class TestDataSetOpenClose extends TestCase {
    private Random _rand = new Random();
    private DataSet<byte[]> _set;
    
    @Override
    protected void setUp() {
        try {
            File homeDir = DirUtils.getTestDir(TestDataSetOpenClose.class);
            int initialCapacity = 1000 + _rand.nextInt(1000);
            _set = createDataSet(homeDir, initialCapacity);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            if(_set != null) {
                _set.close();
            }
            
            File homeDir = DirUtils.getTestDir(TestDataSetOpenClose.class);
            DirUtils.deleteDirectory(homeDir);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    protected DataSet<byte[]> createDataSet(File homeDir, int initialCapacity) throws Exception {
        StoreConfig config = new StoreConfig(homeDir, initialCapacity);
        config.setSegmentFactory(new MemorySegmentFactory());
        config.setSegmentFileSizeMB(32);
        
        return StoreFactory.createDynamicDataSet(config);
    }
    
    /**
     * Populates the underlying data store.
     */
    protected void populate(int num) throws Exception {
        for (int i = 0; i < num; i++) {
            String str = "value." + i;
            byte[] value = str.getBytes();
            _set.add(value);
        }
        _set.sync();
    }
    
    /**
     * Check if the number of keys match the expected number.
     */
    protected void count(int expected) {
        int count = 0;
        Iterator<byte[]> itr = _set.iterator();
        
        while(itr.hasNext()) {
            itr.next();
            count++;
        }
        
        assertEquals(expected, count);
    }
    
    /**
     * Test the number of keys upon opening/closing a data store.
     */
    public void testOpenClose() throws Exception {
        int num;
        
        // Populate data store
        num = 1000 + _rand.nextInt(1000);
        populate(num);
        count(num);
        
        num += 30000 + _rand.nextInt(10000);
        populate(num);
        count(num);
        
        _set.close();
        _set.open();
        count(num);
        
        num += 500000 + _rand.nextInt(100000);
        populate(num);
        count(num);
        
        _set.close();
        _set.open();
        count(num);
        
        _set.close();
    }
}
