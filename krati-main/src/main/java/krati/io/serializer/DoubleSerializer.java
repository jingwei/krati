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
 * DoubleSerializer
 * 
 * @author jwu
 * @since 10/01, 2012
 */
public class DoubleSerializer implements Serializer<Double> {
    private final ByteOrder _byteOrder;
    
    /**
     * Creates a new double Serializer using the BIG_ENDIAN byte order.
     */
    public DoubleSerializer() {
        this._byteOrder = ByteOrder.BIG_ENDIAN;
    }
    
    /**
     * Creates a new double Serializer using the specified byte order.
     */
    public DoubleSerializer(ByteOrder byteOrder) {
        this._byteOrder = (byteOrder == null) ? ByteOrder.BIG_ENDIAN : byteOrder;
    }
    
    @Override
    public byte[] serialize(Double value) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.doubleBytesBE(value) : Numbers.doubleBytesLE(value);
    }
    
    @Override
    public Double deserialize(byte[] bytes) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.doubleValueBE(bytes) : Numbers.doubleValueLE(bytes);
    }
    
    public double doubleValue(byte[] bytes) {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.doubleValueBE(bytes) : Numbers.doubleValueLE(bytes);
    }
    
    public byte[] doubleBytes(double value) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.doubleBytesBE(value) : Numbers.doubleBytesLE(value);
    }
}
