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
 * ShortSerializer
 * 
 * @author jwu
 * @since 10/01, 2012
 */
public class ShortSerializer implements Serializer<Short> {
    private final ByteOrder _byteOrder;
    
    /**
     * Creates a new short Serializer using the BIG_ENDIAN byte order.
     */
    public ShortSerializer() {
        this._byteOrder = ByteOrder.BIG_ENDIAN;
    }
    
    /**
     * Creates a new short Serializer using the specified byte order.
     */
    public ShortSerializer(ByteOrder byteOrder) {
        this._byteOrder = (byteOrder == null) ? ByteOrder.BIG_ENDIAN : byteOrder;
    }
    
    @Override
    public byte[] serialize(Short value) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.shortBytesBE(value) : Numbers.shortBytesLE(value);
    }
    
    @Override
    public Short deserialize(byte[] bytes) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.shortValueBE(bytes) : Numbers.shortValueLE(bytes);
    }
    
    public short shortValue(byte[] bytes) {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.shortValueBE(bytes) : Numbers.shortValueLE(bytes);
    }
    
    public byte[] shortBytes(Short value) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.shortBytesBE(value) : Numbers.shortBytesLE(value);
    }
}
