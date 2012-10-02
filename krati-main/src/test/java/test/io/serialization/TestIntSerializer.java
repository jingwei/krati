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

package test.io.serialization;

import krati.io.Serializer;
import krati.io.serializer.IntSerializer;

/**
 * TestIntSerializer
 * 
 * @author jwu
 * @since 10/02, 2012
 */
public class TestIntSerializer extends AbstractTestNumberSerializer<Integer> {
    
    @Override
    protected IntSerializer createSerializer() {
        return new IntSerializer();
    }

    @Override
    protected Integer anyValue() {
        return _rand.nextInt();
    }

    @Override
    protected Integer minValue() {
        return Integer.MIN_VALUE;
    }

    @Override
    protected Integer maxValue() {
        return Integer.MAX_VALUE;
    }
    
    public void testCustomApi() {
        IntSerializer serializer = createSerializer();
        
        int val = 0;
        assertEquals(val, serializer.intValue(serializer.intBytes(val)));
        val = minValue();
        assertEquals(val, serializer.intValue(serializer.intBytes(val)));
        val = maxValue();
        assertEquals(val, serializer.intValue(serializer.intBytes(val)));
        val = anyValue();
        assertEquals(val, serializer.intValue(serializer.intBytes(val)));
    }
}
