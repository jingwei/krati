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

import java.util.List;
import java.util.Map.Entry;

/**
 * DataStoreHandler
 * 
 * @author jwu
 * 
 * <p>
 * 06/13, 2012 - Added javadoc <br/>
 */
public interface DataStoreHandler extends DataHandler {
    
    /**
     * Assembles the specified <code>key</code> and <code>value</code> into a byte array.
     * 
     * @param key   - the key
     * @param value - the value
     * @return the resulting byte array
     */
    public byte[] assemble(byte[] key, byte[] value);
    
    /**
     * Assembles the specified <code>key</code> and <code>value</code> into a byte array
     * which is then combined with the specified <code>data</code> into the resulting byte array.
     * 
     * @param key   - the key
     * @param value - the value
     * @param data  - the assembled data
     * @return the resulting byte array
     */
    public byte[] assemble(byte[] key, byte[] value, byte[] data);
    
    /**
     * Counts the number of collisions of the specified <code>key</code> in the <code>data</code>.
     * 
     * @param key  - the key
     * @param data - the assembled data
     * @return the number of keys found in the specified <code>data</code> if the specified <code>key</code>
     * is found. Otherwise, the negative number of keys found in the <code>data</code>.
     */
    public int countCollisions(byte[] key, byte[] data);
    
    /**
     * Extracts the value mapped to the specified <code>key</code>.
     * 
     * @param key  - the key
     * @param data - the assembled data
     * @return the value
     */
    public byte[] extractByKey(byte[] key, byte[] data);
    
    /**
     * Removes the specified <code>key</code> and its value from the specified <code>data</code>.
     *  
     * @param key  - the key
     * @param data - the assembled data
     * @return the number of bytes left in the <code>data</code> after removing the <code>key</code> and its value.
     */
    public int removeByKey(byte[] key, byte[] data);
    
    /**
     * Extracts the keys from the specified <code>data</code>.
     * 
     * @param data - the assembled data
     * @return a list of keys.
     */
    public List<byte[]> extractKeys(byte[] data);
    
    /**
     * Extracts the values from the specified <code>data</code>.
     * 
     * @param data - the assembled data
     * @return a list of values.
     */
    public List<byte[]> extractValues(byte[] data);
    
    /**
     * Extracts the mappings from the specified <code>data</code>.
     * 
     * @param data - the assembled data
     * @return a list of Map.Entry values.
     */
    public List<Entry<byte[], byte[]>> extractEntries(byte[] data);
    
    /**
     * Assembles a list of mappings into a byte array.
     * 
     * @param entries - the list of mappings
     * @return the resulting byte array
     */
    public byte[] assembleEntries(List<Entry<byte[], byte[]>> entries);
}
