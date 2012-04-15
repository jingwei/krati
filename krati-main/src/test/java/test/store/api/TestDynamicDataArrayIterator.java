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

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.store.ArrayStore;

/**
 * TestDynamicDataArrayIterator
 * 
 * @author jwu
 * @since 10/08, 2011
 */
public class TestDynamicDataArrayIterator extends AbstractTestArrayStoreIterator {
    
    @Override
    protected ArrayStore createStore(File homeDir) throws Exception {
        StoreConfig config = new StoreConfig(homeDir, 1000);
        return StoreFactory.createDynamicArrayStore(config);
    }
}
