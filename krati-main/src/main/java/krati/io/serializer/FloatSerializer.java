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

package krati.io.serializer;

import java.nio.ByteOrder;

import krati.io.SerializationException;
import krati.io.Serializer;
import krati.util.Numbers;

/**
 * FloatSerializer
 * 
 * @author jwu
 * @since 10/01, 2012
 */
public class FloatSerializer implements Serializer<Float> {
    private final ByteOrder _byteOrder;
    
    /**
     * Creates a new float Serializer using the BIG_ENDIAN byte order.
     */
    public FloatSerializer() {
        this._byteOrder = ByteOrder.BIG_ENDIAN;
    }
    
    /**
     * Creates a new float Serializer using the specified byte order.
     */
    public FloatSerializer(ByteOrder byteOrder) {
        this._byteOrder = (byteOrder == null) ? ByteOrder.BIG_ENDIAN : byteOrder;
    }
    
    @Override
    public byte[] serialize(Float value) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.floatBytesBE(value) : Numbers.floatBytesLE(value);
    }
    
    @Override
    public Float deserialize(byte[] bytes) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.floatValueBE(bytes) : Numbers.floatValueLE(bytes);
    }
    
    public float floatValue(byte[] bytes) {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.floatValueBE(bytes) : Numbers.floatValueLE(bytes);
    }
    
    public byte[] floatBytes(float value) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.floatBytesBE(value) : Numbers.floatBytesLE(value);
    }
}
