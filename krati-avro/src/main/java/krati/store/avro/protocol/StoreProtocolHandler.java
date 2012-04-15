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

/**
 * StoreProtocolHandler defines an interface for handling different messages from the store transport protocol
 * (see {@link krati.store.avro.protocol.Protocols#getProtocol() Protocols}).
 * For message <tt>meta</tt>, properties are received in the form of {@link java.lang.String String}.
 * For messages <tt>get</tt>/<tt>put</tt>/<tt>delete</tt>, keys and values are received in the form of byte array.
 *  
 * @author jwu
 * @since 09/22, 2011
 */
public interface StoreProtocolHandler {
    
    /**
     * Gets the meta value according to the specified meta key.
     * 
     * @param opt   - meta option
     * @param key   - meta key
     * @param value - meta value
     * @return the meta value associated with the meta key.
     * @throws Exception if the meta key can not be handled for any reasons.
     */
    public String meta(String opt, String key, String value) throws Exception;
    
    /**
     * Gets the value to which the specified <code>key</code> is mapped.
     * 
     * @param key - the key
     * @return the value associated with the <code>key</code>,
     *         or <code>null</code> if the <code>key</code> is not known.
     */
    public byte[] get(byte[] key);
    
    /**
     * Maps the specified <code>key</code> to the specified <code>value</code>.
     * 
     * @param key   - the key
     * @param value - the value
     * @return <code>true</code> if this operation is completed successfully.
     *         Otherwise, <cod>false</code>.
     * @throws Exception if this operation cannot be completed for any reasons.
     */
    public boolean put(byte[] key, byte[] value) throws Exception;
    
    /**
     * Removes the specified <code>key</code>.
     * 
     * @param key   - the key
     * @return <code>true</code> if this operation is completed successfully.
     *         Otherwise, <cod>false</code>.
     * @throws Exception if this operation cannot be completed for any reasons.
     */
    public boolean delete(byte[] key) throws Exception;
}
