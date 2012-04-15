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

package krati.store.factory;

import java.io.IOException;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.store.StaticDataStore;

/**
 * StaticDataStoreFactory creates a {@link StaticDataStore} with keys and values
 * in the form of byte array.
 * 
 * @author jwu
 * @since 12/05, 2011
 */
public class StaticDataStoreFactory implements DataStoreFactory {
    
    /**
     * Creates a {@link StaticDataStore} with keys and values in the form of byte array.
     * 
     * @param config - the store configuration
     * @return the newly created StaticDataStore. 
     * @throws IOException if the store cannot be created.
     */
    @Override
    public StaticDataStore create(StoreConfig config) throws IOException {
        try {
            return StoreFactory.createStaticDataStore(config);
        } catch (Exception e) {
            if(e instanceof IOException) {
                throw (IOException)e;
            } else {
                throw new IOException(e);
            }
        }
    }
}
