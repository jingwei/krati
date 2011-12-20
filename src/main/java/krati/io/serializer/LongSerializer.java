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

import java.nio.ByteOrder;

import krati.io.SerializationException;
import krati.io.Serializer;
import krati.util.Numbers;

/**
 * LongSerializer
 * 
 * @author jwu
 * @since 09/25, 2011
 */
public class LongSerializer implements Serializer<Long> {
    private final ByteOrder _byteOrder;
    
    /**
     * Creates a new long Serializer using the BIG_ENDIAN byte order.
     */
    public LongSerializer() {
        this._byteOrder = ByteOrder.BIG_ENDIAN;
    }
    
    /**
     * Creates a new long Serializer using the specified byte order.
     */
    public LongSerializer(ByteOrder byteOrder) {
        this._byteOrder = (byteOrder == null) ? ByteOrder.BIG_ENDIAN : byteOrder;
    }
    
    @Override
    public Long deserialize(byte[] bytes) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.longValueBE(bytes) : Numbers.longValueLE(bytes);
    }
    
    @Override
    public byte[] serialize(Long value) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.longBytesBE(value) : Numbers.longBytesLE(value);
    }
    
    public long longValue(byte[] bytes) {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.longValueBE(bytes) : Numbers.longValueLE(bytes);
    }
    
    public byte[] longBytes(long value) {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.longBytesBE(value) : Numbers.longBytesLE(value);
    }
}
