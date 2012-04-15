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

import junit.framework.TestCase;
import krati.io.Serializer;
import krati.io.serializer.StringSerializer;
import krati.io.serializer.StringSerializerUtf8;

/**
 * TestSerializer
 * 
 * @author jwu
 * @since 09/25, 2011
 */
public class TestSerializer extends TestCase {
    
    public void testStringSerializer() {
        Serializer<String> serializer = new StringSerializer();
        
        String str1 = TestSerializer.class.getSimpleName();
        String str2 = serializer.deserialize(serializer.serialize(str1));
        assertEquals(str1, str2);
    }
    
    public void testStringSerializerUtf8() {
        Serializer<String> serializer = new StringSerializerUtf8();
        
        String str1 = TestSerializer.class.getSimpleName();
        String str2 = serializer.deserialize(serializer.serialize(str1));
        assertEquals(str1, str2);
    }
}
