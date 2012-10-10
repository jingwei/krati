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
 * Test BytesDB Segment Index Buffer is auto-enabled.
 * 
 * @author jwu
 * @since 09/04, 2012
 */
public class TestBytesDBSib extends TestCase {
    protected BytesDB _bytesDB;
    protected final Random _rand = new Random();
    protected String quote = "All work and no play makes Jack a dull boy. All play and no work makes Jack a mere toy.";
    
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
            config.setSegmentFileSizeMB(8);
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
    
    public void testSibEnabled() throws Exception {
        int index;
        File segsDir = new File(_bytesDB.getHomeDir(), "segs");
        
        int count1 = getInitialCapacity();
        for(int i = 0; i < count1; i++) {
            index = _bytesDB.add(("index." + i + "=" + quote).getBytes(), System.currentTimeMillis());
            assertTrue(_bytesDB.hasIndex(index));
        }
        
        int count2 = count1 * (20 + _rand.nextInt(10));
        for(int i = 0; i < count2; i++) {
            index = _rand.nextInt(count1);
            _bytesDB.set(index, ("index." + index + "=" + quote).getBytes(), System.currentTimeMillis());
        }
        
        int segCount1 = FileUtils.countFiles(segsDir, ".seg");
        int sibCount1 = FileUtils.countFiles(segsDir, ".sib");
        assertTrue(segCount1 >= sibCount1);
        
        _bytesDB.close();
        
        int segCount2 = FileUtils.countFiles(segsDir, ".seg");
        int sibCount2 = FileUtils.countFiles(segsDir, ".sib");
        assertTrue(segCount2 >= sibCount2);
        
        assertEquals(segCount1, segCount2);
        assertTrue(sibCount1 <= sibCount2);
        assertTrue(sibCount2 > 0);
    }
}
