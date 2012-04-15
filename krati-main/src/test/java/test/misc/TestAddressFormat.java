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

package test.misc;

import krati.core.array.basic.DynamicConstants;
import krati.core.segment.AddressFormat;
import test.AbstractTest;

/**
 * TestAddressFormat
 * 
 * @author jwu
 * 
 */
public class TestAddressFormat extends AbstractTest {

    public TestAddressFormat() {
        super(TestAddressFormat.class.getSimpleName());
    }
    
    public void testAddressFormat() {
        int offset, segment, dataSize;
        AddressFormat af = new AddressFormat();
        
        // Test 1
        offset = 12345;
        segment = 24;
        dataSize = (1 << DynamicConstants.SUB_ARRAY_BITS) - 1;
        check(af, offset, segment, dataSize);
        
        // Test 2
        offset = 12345;
        segment = 24;
        dataSize = (1 << DynamicConstants.SUB_ARRAY_BITS);
        check(af, offset, segment, dataSize);
        
        // Test 3
        offset = 12345;
        segment = 24;
        dataSize = 0;
        check(af, offset, segment, dataSize);
        
        // Test 4
        offset = Integer.MAX_VALUE;
        segment = 24;
        dataSize = 1023;
        check(af, offset, segment, dataSize);
        
        // Test 5
        offset = 0;
        segment = 24;
        dataSize = 1023;
        check(af, offset, segment, dataSize);
        
        // Test 6
        offset = 0;
        segment = 24;
        dataSize = af.getMaxDataSize();
        check(af, offset, segment, dataSize);
    }
    
    private void check(AddressFormat af, int offset, int segment, int dataSize) {
        long addr = af.composeAddress(offset, segment, dataSize);
        int offset2 = af.getOffset(addr);
        int segment2 = af.getSegment(addr);
        int dataSize2 = af.getDataSize(addr);
        
        assertEquals("offset", offset, offset2);
        assertEquals("segment", segment, segment2);
        assertEquals("dataSize", (dataSize > af.getMaxDataSize()) ? 0 : dataSize, dataSize2);
    }
}
