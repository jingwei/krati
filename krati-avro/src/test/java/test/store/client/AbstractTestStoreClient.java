/*
 * Copyright (c) 2011 LinkedIn, Inc
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

package test.store.client;

import java.io.File;
import java.io.IOException;

import test.util.DirUtils;
import test.util.Utility;

import junit.framework.TestCase;
import krati.store.DataStore;
import krati.store.avro.client.StoreClient;
import krati.store.avro.protocol.BasicDataStoreResponder;
import krati.store.avro.protocol.StoreResponder;

/**
 * AbstractTestStoreClient
 * 
 * @author jwu
 * @since 10/03, 2011
 */
public abstract class AbstractTestStoreClient<K, V> extends TestCase {
    private DataStore<byte[], byte[]> _store;
    
    protected int getInitialCapacity() {
        return 10000;
    }
    
    protected DataStore<byte[], byte[]> createDataStore(File storeDir, int initialCapacity) throws Exception {
        return Utility.createStaticDataStore(storeDir, initialCapacity);
    }
    
    protected StoreResponder createStoreResponder() {
        return new BasicDataStoreResponder(_store);
    }
    
    protected abstract StoreClient<K, V> createStoreClient();
    
    @Override
    protected void setUp() {
        try {
            _store = createDataStore(DirUtils.getTestDir(getClass()), getInitialCapacity());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            _store.close();
            DirUtils.deleteDirectory(DirUtils.getTestDir(getClass()));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            _store = null;
        }
    }
}
