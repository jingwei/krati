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

package krati.store;

/**
 * StoreWriter
 * 
 * @author jwu
 * @since 09/15, 2011
 */
public interface StoreWriter<K, V> {
    
    /**
     * Maps the specified <code>key</code> to the specified <code>value</code> in this store.
     * 
     * @param key   - the key
     * @param value - the value
     * @return <code>true</code> if this store is changed as a result of this operation.
     *         Otherwise, <cod>false</code>.
     * @throws Exception if this operation cannot be completed.
     */
    public boolean put(K key, V value) throws Exception;
    
    /**
     * Removes the specified <code>key</code> from this store.
     * 
     * @param key   - the key
     * @return <code>true</code> if this store is changed as a result of this operation.
     *         Otherwise, <cod>false</code>.
     * @throws Exception if this operation cannot be completed.
     */
    public boolean delete(K key) throws Exception;
    
}
