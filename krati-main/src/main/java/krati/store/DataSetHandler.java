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

/**
 * DataSetHandler
 * 
 * @author jwu
 * 
 */
public interface DataSetHandler extends DataHandler {
    
    /**
     * Counts the number of values in the specified <code>data</code>
     *  
     * @param data - the assembled data
     * @return the number of values.
     */
    public int count(byte[] data);
    
    /**
     * Assembles the specified value into a byte array.
     * 
     * @param value - the value
     * @return the resulting byte array.
     */
    public byte[] assemble(byte[] value);
    
    /**
     * Assembles the specified <code>value</code> with the specified <code>data</code>.
     * 
     * @param value - the value
     * @param data  - the assembled data
     * @return the resulting byte array
     */
    public byte[] assemble(byte[] value, byte[] data);
    
    /**
     * Counts the number of collisions of the specified <code>value</code> in the <code>data</code>.
     * 
     * @param value - the value
     * @param data  - the assembled data
     * @return the number of values found in the specified <code>data</code> if the specified <code>value</code>
     * is found. Otherwise, the negative number of values found in the <code>data</code>.
     */
    public int countCollisions(byte[] value, byte[] data);
    
    /**
     * Removes the specified <code>value</code> from the assembled <code>data</code>.
     * 
     * @param value - the value
     * @param data  - the assembled data
     * @return the number of bytes left in the <code>data</code> after removing the <code>value</code>.
     */
    public int remove(byte[] value, byte[] data);
    
    /**
     * Finds the specified <code>value</code> from the assembled <code>data</code>.
     * 
     * @param value - the value
     * @param data  - the assembled data
     * @return <code>true</code> if the <code>value</code> is found. Otherwise, <code>false</code>.
     */
    public boolean find(byte[] value, byte[] data);
    
    /**
     * Extracts the values from the assembled <code>data</code>.
     * 
     * @param data the assembled data
     * @return a list of values. 
     */
    public List<byte[]> extractValues(byte[] data);
}
