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

import krati.store.DataStore;
import krati.store.StaticDataStore;

/**
 * TestIteratorStaticDataStore
 * 
 * @author jwu
 * Sep 30, 2010
 */
public class TestIteratorStaticDataStore  extends EvalDataStoreIterator {

    public TestIteratorStaticDataStore() throws Exception {
        super(TestIteratorStaticDataStore.class.getSimpleName());
    }
    
    protected DataStore<byte[], byte[]> createDataStore(File storeDir) throws Exception {
        int capacity = (int)(_keyCount * 1.5);
        return new StaticDataStore(storeDir,
                                   capacity, /* capacity */
                                   10000,    /* batchSize */
                                   5,        /* numSyncBatches */
                                   _segFileSizeMB,
                                   createSegmentFactory());
    }
}
