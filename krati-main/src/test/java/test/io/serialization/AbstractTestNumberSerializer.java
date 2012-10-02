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

import java.util.Arrays;

import krati.io.Serializer;

/**
 * AbstractTestNumberSerializer
 * 
 * @author jwu
 * @since 10/02, 2012
 */
public abstract class AbstractTestNumberSerializer<T> extends AbstractTestSerializer<T> {
    
    @Override
    protected T createObject() {
        return anyValue();
    }
    
    protected abstract T anyValue();
    
    protected abstract T minValue();
    
    protected abstract T maxValue();
    
    protected void checkSerialization(Serializer<T> serializer, T object1) {
        byte[] bytes1 = serializer.serialize(object1);
        T object2 = serializer.deserialize(bytes1);
        byte[] bytes2 = serializer.serialize(object2);
        
        assertTrue(Arrays.equals(bytes1, bytes2));
    }
    
    public void testMinMaxValues() {
        Serializer<T> serializer = createSerializer();
        checkSerialization(serializer, minValue());
        checkSerialization(serializer, maxValue());
    }
}
