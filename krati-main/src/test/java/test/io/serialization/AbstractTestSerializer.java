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

package test.io.serialization;

import java.util.Arrays;

import junit.framework.TestCase;
import krati.io.Serializer;

/**
 * AbstractTestSerializer
 * 
 * @author jwu
 * 07/18, 2011
 * 
 * @param <T> Object to serialize
 */
public abstract class AbstractTestSerializer<T> extends TestCase {
    
    protected abstract T createObject();
    
    protected abstract Serializer<T> createSerializer();
    
    public void testApiBasics() {
        Serializer<T> serializer = createSerializer();
        
        T object1 = createObject();
        byte[] bytes1 = serializer.serialize(object1);
        
        T object2 = serializer.deserialize(bytes1);
        byte[] bytes2 = serializer.serialize(object2);
        
        assertTrue(Arrays.equals(bytes1, bytes2));
    }
}
