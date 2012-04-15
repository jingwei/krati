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

package krati.io.serializer;

import java.nio.charset.Charset;

import krati.io.SerializationException;
import krati.io.Serializer;

/**
 * StringSerializerUtf8
 * 
 * @author jwu
 * @since 09/25, 2011
 */
public class StringSerializerUtf8 implements Serializer<String> {
    private final static Charset UTF8 = Charset.forName("UTF-8");
    
    /**
     * Deserialize a byte array to String using the UTF-8 charset.
     * 
     * @throws NullPointerException if the <tt>bytes</tt> is null.
     */
    @Override
    public String deserialize(byte[] bytes) throws SerializationException {
        return new String(bytes, UTF8);
    }
    
    /**
     * Serialize a String to a byte array using the UTF-8 charset.
     * 
     * @throws NullPointerException if the <tt>str</tt> is null.
     */
    @Override
    public byte[] serialize(String str) throws SerializationException {
        return str.getBytes(UTF8);
    }
}
