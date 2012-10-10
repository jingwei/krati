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

import krati.io.serializer.FloatSerializer;

/**
 * TestFloatSerializer
 * 
 * @author jwu
 * @since 10/02, 2012
 */
public class TestFloatSerializer extends AbstractTestNumberSerializer<Float> {

    @Override
    protected FloatSerializer createSerializer() {
        return new FloatSerializer();
    }

    @Override
    protected Float anyValue() {
        return _rand.nextFloat();
    }

    @Override
    protected Float minValue() {
        return Float.MIN_VALUE;
    }

    @Override
    protected Float maxValue() {
        return Float.MAX_VALUE;
    }

    public void testCustomApi() {
        FloatSerializer serializer = createSerializer();
        
        float val = 0;
        assertEquals(val, serializer.floatValue(serializer.floatBytes(val)));
        val = minValue();
        assertEquals(val, serializer.floatValue(serializer.floatBytes(val)));
        val = maxValue();
        assertEquals(val, serializer.floatValue(serializer.floatBytes(val)));
        val = anyValue();
        assertEquals(val, serializer.floatValue(serializer.floatBytes(val)));
    }
}
