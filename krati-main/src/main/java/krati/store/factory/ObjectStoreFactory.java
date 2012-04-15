/*
 * Copyright (c) 2010-2011 LinkedIn, Inc
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
import krati.io.Serializer;
import krati.store.ObjectStore;

/**
 * ObjectStoreFactory defines the interface for creating a {@link ObjectStore}.
 * 
 * @author jwu
 * @since 10/11, 2011
 */
public interface ObjectStoreFactory<K, V> {
    
    /**
     * Create an instance of {@link ObjectStore} for mapping keys to values.
     * 
     * @param config          - the configuration
     * @param keySerializer   - the serializer for keys
     * @param valueSerializer - the serializer for values
     * @return the newly created store
     * @throws IOException if the store cannot be created.
     */
    public ObjectStore<K, V> create(StoreConfig config, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException;
}
