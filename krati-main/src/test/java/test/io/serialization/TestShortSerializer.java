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

import krati.io.serializer.ShortSerializer;

/**
 * TestShortSerializer
 * 
 * @author jwu
 * @since 10/02, 2012
 */
public class TestShortSerializer extends AbstractTestNumberSerializer<Short> {

    @Override
    protected ShortSerializer createSerializer() {
        return new ShortSerializer();
    }
    
    @Override
    protected Short anyValue() {
        return (short)_rand.nextInt();
    }

    @Override
    protected Short minValue() {
        return Short.MIN_VALUE;
    }

    @Override
    protected Short maxValue() {
        return Short.MAX_VALUE;
    }

    public void testCustomApi() {
        ShortSerializer serializer = createSerializer();
        
        short val = 0;
        assertEquals(val, serializer.shortValue(serializer.shortBytes(val)));
        val = minValue();
        assertEquals(val, serializer.shortValue(serializer.shortBytes(val)));
        val = maxValue();
        assertEquals(val, serializer.shortValue(serializer.shortBytes(val)));
        val = anyValue();
        assertEquals(val, serializer.shortValue(serializer.shortBytes(val)));
    }
}
