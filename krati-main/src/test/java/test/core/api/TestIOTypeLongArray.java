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

package test.core.api;

import java.io.File;

import krati.array.Array;
import krati.core.array.AddressArray;
import krati.core.array.basic.DynamicConstants;
import krati.core.array.basic.IOTypeLongArray;

/**
 * TestIOTypeLongArray
 * 
 * @author jwu
 * 06/21, 2011
 * 
 */
public class TestIOTypeLongArray extends AbstractTestDynamicAddressArray {
    
    @Override
    protected AddressArray createAddressArray(File homeDir) throws Exception {
        return new IOTypeLongArray(Array.Type.DYNAMIC, DynamicConstants.SUB_ARRAY_SIZE, getBatchSize(), getNumSyncBatches(), homeDir);
    }
    
    public void testCapacity() throws Exception {
        int index;
        int length;
        
        length = _array.length();
        index = length + _rand.nextInt(length);
        _array.expandCapacity(index);
        assertTrue(length < _array.length());
        
        length = _array.length();
        index = length + _rand.nextInt(length);
        _array.expandCapacity(index);
        assertTrue(length < _array.length());
        
        length = _array.length();
        index = length + _rand.nextInt(length);
        _array.expandCapacity(index);
        assertTrue(length < _array.length());
        
        index = Integer.MAX_VALUE;
        _array.expandCapacity(index);
        assertEquals(Integer.MAX_VALUE, _array.length());
    }
}
