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

package krati.store.avro.protocol;

import krati.store.DataStore;

/**
 * BasicDataStoreResponder
 * 
 * @author jwu
 * @since 08/07, 2011
 */
public class BasicDataStoreResponder extends StoreResponder {
    
    public BasicDataStoreResponder(DataStore<byte[], byte[]> store) {
        super(new BasicDataStoreProtocolHandler(store));
    }
    
    /**
     * @return the underlying {@link krati.store.DataStore}.
     */
    public DataStore<byte[], byte[]> getStore() {
        return ((BasicDataStoreProtocolHandler)getHandler()).getStore();
    }
}
