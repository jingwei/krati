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

package krati.store.avro.protocol;

import krati.core.StoreConfig;
import krati.store.DataStore;
import krati.store.factory.DataStoreFactory;

/**
 * BasicDataStoreResponderFactory
 * 
 * @author jwu
 * @since 10/01, 2011
 */
public class BasicDataStoreResponderFactory implements StoreResponderFactory {
    private final DataStoreFactory _storeFactory;
    
    /**
     * Creates a new instance BasicDataStoreResponderFactory .
     */
    public BasicDataStoreResponderFactory(DataStoreFactory storeFactory) {
        this._storeFactory = storeFactory;
    }
    
    @Override
    public BasicDataStoreResponder createResponder(StoreConfig config) throws Exception {
        DataStore<byte[], byte[]> store = _storeFactory.create(config);
        return new BasicDataStoreResponder(store);
    }
}
