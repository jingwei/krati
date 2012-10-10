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

import krati.io.serializer.LongSerializer;

/**
 * TestLongSerializer
 * 
 * @author jwu
 * @since 10/02, 2012
 */
public class TestLongSerializer extends AbstractTestNumberSerializer<Long> {

    @Override
    protected LongSerializer createSerializer() {
        return new LongSerializer();
    }

    @Override
    protected Long anyValue() {
        return _rand.nextLong();
    }

    @Override
    protected Long minValue() {
        return Long.MIN_VALUE;
    }

    @Override
    protected Long maxValue() {
        return Long.MAX_VALUE;
    }

    public void testCustomApi() {
        LongSerializer serializer = createSerializer();
        
        long val = 0;
        assertEquals(val, serializer.longValue(serializer.longBytes(val)));
        val = minValue();
        assertEquals(val, serializer.longValue(serializer.longBytes(val)));
        val = maxValue();
        assertEquals(val, serializer.longValue(serializer.longBytes(val)));
        val = anyValue();
        assertEquals(val, serializer.longValue(serializer.longBytes(val)));
    }
}
