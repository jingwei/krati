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
import krati.store.ArrayStore;

/**
 * ArrayStoreFactory defines the interface for creating a {@link ArrayStore} based on a store configuration.
 * 
 * @author jwu
 * @since 12/06, 2011
 */
public interface ArrayStoreFactory {

    /**
     * Creates a {@link ArrayStore} based on the specified store configuration.
     * 
     * @param config - the store configuration
     * @return the newly created array store. 
     * @throws IOException if the store cannot be created.
     */
    public ArrayStore create(StoreConfig config) throws IOException;
}
