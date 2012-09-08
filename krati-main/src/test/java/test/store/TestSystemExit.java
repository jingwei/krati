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
import java.util.Random;

import test.util.DirUtils;

import junit.framework.TestCase;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.segment.SegmentFactory;
import krati.io.Serializer;
import krati.io.serializer.StringSerializerUtf8;
import krati.store.DataStore;

/**
 * TestSystemExit
 * 
 * @author jwu
 * @since 09/08, 2012
 */
public class TestSystemExit extends TestCase {
    protected Random _rand = new Random();
    protected DataStore<byte[], byte[]> _store;
    protected Serializer<String> serializer = new StringSerializerUtf8(); 
    
    protected int getInitialCapacity() {
        return 1000000;
    }
    
    protected SegmentFactory getSegmentFactory() {
        return new krati.core.segment.WriteBufferSegmentFactory();
    }
    
    protected DataStore<byte[], byte[]> create(File storeDir) throws Exception {
        StoreConfig config = new StoreConfig(storeDir, getInitialCapacity());
        config.setSegmentFileSizeMB(16);
        config.setSegmentFactory(getSegmentFactory());
        config.setBatchSize(5000);
        config.setNumSyncBatches(10);
        
        return StoreFactory.createDynamicDataStore(config);
    }
    
    @Override
    protected void setUp() {
        try {
            _store = create(DirUtils.getTestDir(getClass()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void testSystemExit() throws Exception {
        Iterator<byte[]> keyIter = _store.keyIterator();
        if(!keyIter.hasNext()) {
            int cnt = getInitialCapacity();
            for(int i = 0; i < cnt; i++) {
                byte[] key = serializer.serialize("key." + i);
                byte[] value = serializer.serialize("value." + i);
                _store.put(key, value);
            }
            _store.persist();
            System.out.println("populated " + cnt + " keys");
        }
        
        keyIter = _store.keyIterator();
        {
            int numKeys = 0;
            while(keyIter.hasNext()) {
                keyIter.next();
                numKeys++;
            }
            
            assertEquals(getInitialCapacity(), numKeys);
        }
        
        int cnt = 5000 + _rand.nextInt(getInitialCapacity());
        for(int i = 0; i < cnt; i++) {
            int n = _rand.nextInt(getInitialCapacity());
            byte[] key = serializer.serialize("key." + n);
            byte[] value = serializer.serialize("value." + n);
            _store.put(key, value);
        }
        
        System.out.println("randomput " + cnt + " keys");
        System.exit(0);
    }
}
