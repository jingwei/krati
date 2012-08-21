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
import java.util.Random;

import junit.framework.TestCase;
import krati.core.StoreConfig;
import krati.core.segment.WriteBufferSegmentFactory;
import krati.store.BytesDB;
import test.util.FileUtils;

/**
 * TestBytesDBGrowth
 * 
 * @author jwu
 * @since 08/21, 2012
 */
public class TestBytesDBGrowth extends TestCase {
    protected BytesDB _bytesDB;
    protected final Random _rand = new Random();
    
    protected int getInitialCapacity() {
        return 10000;
    }
    
    @Override
    protected void setUp() {
        try {
            File homeDir = FileUtils.getTestDir(getClass().getSimpleName());
            StoreConfig config = new StoreConfig(homeDir, getInitialCapacity());
            config.setBatchSize(1000);
            config.setNumSyncBatches(5);
            config.setSegmentFileSizeMB(32);
            config.setSegmentFactory(new WriteBufferSegmentFactory());
            _bytesDB = new BytesDB(config);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            if(_bytesDB != null) {
                _bytesDB.close();
                FileUtils.deleteDirectory(_bytesDB.getHomeDir());
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    public void testAutoGrowth() throws Exception {
        int index;
        int count = 800000;
        for(int i = 0; i < count; i++) {
            index = _bytesDB.add(("index=" + i).getBytes(), System.currentTimeMillis());
            assertTrue(_bytesDB.hasIndex(index));
        }
    }
}
