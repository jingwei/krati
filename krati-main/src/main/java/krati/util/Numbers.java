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

package krati.util;

/**
 * Numbers
 * 
 * @author jwu
 * @since 09/25, 2011
 */
public class Numbers {
    
    public static byte[] shortBytesLE(short val) {
        byte[] buffer = new byte[2];
        buffer[0] = (byte) (val);
        buffer[1] = (byte) (val >> 8);
        return buffer;
    }
    
    public static byte[] shortBytesBE(int val) {
        byte[] buffer = new byte[2];
        buffer[1] = (byte) (val);
        buffer[0] = (byte) (val >> 8);
        return buffer;
    }
    
    public static void shortBytesLE(int val, byte[] buffer) {
        buffer[0] = (byte) (val);
        buffer[1] = (byte) (val >> 8);
    }
    
    public static void shortBytesBE(int val, byte[] buffer) {
        buffer[1] = (byte) (val);
        buffer[0] = (byte) (val >> 8);
    }
    
    public static void shortBytesLE(int val, byte[] buffer, int offset) {
        buffer[    offset] = (byte) (val);
        buffer[1 + offset] = (byte) (val >> 8);
    }
    
    public static void shortBytesBE(int val, byte[] buffer, int offset) {
        buffer[1 + offset] = (byte) (val);
        buffer[    offset] = (byte) (val >> 8);
    }
    
    public static byte[] intBytesLE(int val) {
        byte[] buffer = new byte[4];
        buffer[0] = (byte) (val);
        buffer[1] = (byte) (val >> 8);
        buffer[2] = (byte) (val >> 16);
        buffer[3] = (byte) (val >> 24);
        return buffer;
    }
    
    public static byte[] intBytesBE(int val) {
        byte[] buffer = new byte[4];
        buffer[3] = (byte) (val);
        buffer[2] = (byte) (val >> 8);
        buffer[1] = (byte) (val >> 16);
        buffer[0] = (byte) (val >> 24);
        return buffer;
    }
    
    public static void intBytesLE(int val, byte[] buffer) {
        buffer[0] = (byte) (val);
        buffer[1] = (byte) (val >> 8);
        buffer[2] = (byte) (val >> 16);
        buffer[3] = (byte) (val >> 24);
    }
    
    public static void intBytesBE(int val, byte[] buffer) {
        buffer[3] = (byte) (val);
        buffer[2] = (byte) (val >> 8);
        buffer[1] = (byte) (val >> 16);
        buffer[0] = (byte) (val >> 24);
    }
    
    public static void intBytesLE(int val, byte[] buffer, int offset) {
        buffer[    offset] = (byte) (val);
        buffer[1 + offset] = (byte) (val >> 8);
        buffer[2 + offset] = (byte) (val >> 16);
        buffer[3 + offset] = (byte) (val >> 24);
    }
    
    public static void intBytesBE(int val, byte[] buffer, int offset) {
        buffer[3 + offset] = (byte) (val);
        buffer[2 + offset] = (byte) (val >> 8);
        buffer[1 + offset] = (byte) (val >> 16);
        buffer[    offset] = (byte) (val >> 24);
    }
    
    public static byte[] longBytesLE(long val) {
        byte[] buffer = new byte[8];
        buffer[0] = (byte) (val);
        buffer[1] = (byte) (val >> 8);
        buffer[2] = (byte) (val >> 16);
        buffer[3] = (byte) (val >> 24);
        buffer[4] = (byte) (val >> 32);
        buffer[5] = (byte) (val >> 40);
        buffer[6] = (byte) (val >> 48);
        buffer[7] = (byte) (val >> 56);
        return buffer;
    }
    
    public static byte[] longBytesBE(long val) {
        byte[] buffer = new byte[8];
        buffer[7] = (byte) (val);
        buffer[6] = (byte) (val >> 8);
        buffer[5] = (byte) (val >> 16);
        buffer[4] = (byte) (val >> 24);
        buffer[3] = (byte) (val >> 32);
        buffer[2] = (byte) (val >> 40);
        buffer[1] = (byte) (val >> 48);
        buffer[0] = (byte) (val >> 56);
        return buffer;
    }
    
    public static void longBytesLE(long val, byte[] buffer) {
        buffer[0] = (byte) (val);
        buffer[1] = (byte) (val >> 8);
        buffer[2] = (byte) (val >> 16);
        buffer[3] = (byte) (val >> 24);
        buffer[4] = (byte) (val >> 32);
        buffer[5] = (byte) (val >> 40);
        buffer[6] = (byte) (val >> 48);
        buffer[7] = (byte) (val >> 56);
    }
    
    public static void longBytesBE(long val, byte[] buffer) {
        buffer[7] = (byte) (val);
        buffer[6] = (byte) (val >> 8);
        buffer[5] = (byte) (val >> 16);
        buffer[4] = (byte) (val >> 24);
        buffer[3] = (byte) (val >> 32);
        buffer[2] = (byte) (val >> 40);
        buffer[1] = (byte) (val >> 48);
        buffer[0] = (byte) (val >> 56);
    }
    
    public static void longBytesLE(long val, byte[] buffer, int offset) {
        buffer[    offset] = (byte) (val);
        buffer[1 + offset] = (byte) (val >> 8);
        buffer[2 + offset] = (byte) (val >> 16);
        buffer[3 + offset] = (byte) (val >> 24);
        buffer[4 + offset] = (byte) (val >> 32);
        buffer[5 + offset] = (byte) (val >> 40);
        buffer[6 + offset] = (byte) (val >> 48);
        buffer[7 + offset] = (byte) (val >> 56);
    }
    
    public static void longBytesBE(long val, byte[] buffer, int offset) {
        buffer[7 + offset] = (byte) (val);
        buffer[6 + offset] = (byte) (val >> 8);
        buffer[5 + offset] = (byte) (val >> 16);
        buffer[4 + offset] = (byte) (val >> 24);
        buffer[3 + offset] = (byte) (val >> 32);
        buffer[2 + offset] = (byte) (val >> 40);
        buffer[1 + offset] = (byte) (val >> 48);
        buffer[    offset] = (byte) (val >> 56);
    }
    
    public static short shortValueLE(byte[] bytes) {
        return (short) (((bytes[1] & 0xFF) << 8) |
                        ((bytes[0] & 0xFF)));
    }
    
    public static short shortValueBE(byte[] bytes) {
        return (short) (((bytes[0] & 0xFF) << 8) |
                        ((bytes[1] & 0xFF)));
    }
    
    public static short shortValueLE(byte[] bytes, int offset) {
        return (short) (((bytes[1 + offset] & 0xFF) << 8) |
                        ((bytes[    offset] & 0xFF)));
    }
    
    public static short shortValueBE(byte[] bytes, int offset) {
        return (short) (((bytes[    offset] & 0xFF) << 8) |
                        ((bytes[1 + offset] & 0xFF)));
    }
    
    public static int intValueLE(byte[] bytes) {
        return (((bytes[3] & 0xFF) << 24) |
                ((bytes[2] & 0xFF) << 16) |
                ((bytes[1] & 0xFF) << 8) |
                ((bytes[0] & 0xFF)));
    }
    
    public static int intValueBE(byte[] bytes) {
        return (((bytes[0] & 0xFF) << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8) |
                ((bytes[3] & 0xFF)));
    }
    
    public static int intValueLE(byte[] bytes, int offset) {
        return (((bytes[3 + offset] & 0xFF) << 24) |
                ((bytes[2 + offset] & 0xFF) << 16) |
                ((bytes[1 + offset] & 0xFF) << 8) |
                ((bytes[    offset] & 0xFF)));
    }
    
    public static int intValueBE(byte[] bytes, int offset) {
        return (((bytes[    offset] & 0xFF) << 24) |
                ((bytes[1 + offset] & 0xFF) << 16) |
                ((bytes[2 + offset] & 0xFF) << 8) |
                ((bytes[3 + offset] & 0xFF)));
    }
    
    public static long longValueLE(byte[] bytes) {
        return (((long) (bytes[7] & 0xFF) << 56) |
                ((long) (bytes[6] & 0xFF) << 48) |
                ((long) (bytes[5] & 0xFF) << 40) |
                ((long) (bytes[4] & 0xFF) << 32) |
                ((long) (bytes[3] & 0xFF) << 24) |
                ((long) (bytes[2] & 0xFF) << 16) |
                ((long) (bytes[1] & 0xFF) << 8) |
                ((long) (bytes[0] & 0xFF)));
    }
    
    public static long longValueBE(byte[] bytes) {
        return (((long) (bytes[0] & 0xFF) << 56) |
                ((long) (bytes[1] & 0xFF) << 48) |
                ((long) (bytes[2] & 0xFF) << 40) |
                ((long) (bytes[3] & 0xFF) << 32) |
                ((long) (bytes[4] & 0xFF) << 24) |
                ((long) (bytes[5] & 0xFF) << 16) |
                ((long) (bytes[6] & 0xFF) << 8) |
                ((long) (bytes[7] & 0xFF)));
    }
    
    public static long longValueLE(byte[] bytes, int offset) {
        return (((long) (bytes[7 + offset] & 0xFF) << 56) |
                ((long) (bytes[6 + offset] & 0xFF) << 48) |
                ((long) (bytes[5 + offset] & 0xFF) << 40) |
                ((long) (bytes[4 + offset] & 0xFF) << 32) |
                ((long) (bytes[3 + offset] & 0xFF) << 24) |
                ((long) (bytes[2 + offset] & 0xFF) << 16) |
                ((long) (bytes[1 + offset] & 0xFF) << 8) |
                ((long) (bytes[    offset] & 0xFF)));
    }
    
    public static long longValueBE(byte[] bytes, int offset) {
        return (((long) (bytes[    offset] & 0xFF) << 56) |
                ((long) (bytes[1 + offset] & 0xFF) << 48) |
                ((long) (bytes[2 + offset] & 0xFF) << 40) |
                ((long) (bytes[3 + offset] & 0xFF) << 32) |
                ((long) (bytes[4 + offset] & 0xFF) << 24) |
                ((long) (bytes[5 + offset] & 0xFF) << 16) |
                ((long) (bytes[6 + offset] & 0xFF) << 8) |
                ((long) (bytes[7 + offset] & 0xFF)));
    }
    
    public static byte[] floatBytesLE(float val) {
        int v = Float.floatToIntBits(val);
        return intBytesLE(v);
    }
    
    public static byte[] floatBytesBE(float val) {
        int v = Float.floatToIntBits(val);
        return intBytesBE(v);
    }
    
    public static void floatBytesLE(float val, byte[] buffer) {
        int v = Float.floatToIntBits(val);
        intBytesLE(v, buffer);
    }
    
    public static void floatBytesBE(float val, byte[] buffer) {
        int v = Float.floatToIntBits(val);
        intBytesBE(v, buffer);
    }
    
    public static void floatBytesLE(float val, byte[] buffer, int offset) {
        int v = Float.floatToIntBits(val);
        intBytesLE(v, buffer, offset);
    }
    
    public static void floatBytesBE(float val, byte[] buffer, int offset) {
        int v = Float.floatToIntBits(val);
        intBytesBE(v, buffer, offset);
    }
    
    public static float floatValueLE(byte[] bytes) {
        int bits = intValueLE(bytes);
        return Float.intBitsToFloat(bits);
    }
    
    public static float floatValueBE(byte[] bytes) {
        int bits = intValueBE(bytes);
        return Float.intBitsToFloat(bits);
    }
    
    public static float floatValueLE(byte[] bytes, int offset) {
        int bits = intValueLE(bytes, offset);
        return Float.intBitsToFloat(bits);
    }
    
    public static float floatValueBE(byte[] bytes, int offset) {
        int bits = intValueBE(bytes, offset);
        return Float.intBitsToFloat(bits);
    }
    
    public static byte[] doubleBytesLE(double val) {
        long v = Double.doubleToLongBits(val);
        return longBytesLE(v);
    }
    
    public static byte[] doubleBytesBE(double val) {
        long v = Double.doubleToLongBits(val);
        return longBytesBE(v);
    }
    
    public static void doubleBytesLE(double val, byte[] buffer) {
        long v = Double.doubleToLongBits(val);
        longBytesLE(v, buffer);
    }
    
    public static void doubleBytesBE(double val, byte[] buffer) {
        long v = Double.doubleToLongBits(val);
        longBytesBE(v, buffer);
    }
    
    public static void doubleBytesLE(double val, byte[] buffer, int offset) {
        long v = Double.doubleToLongBits(val);
        longBytesLE(v, buffer, offset);
    }
    
    public static void doubleBytesBE(double val, byte[] buffer, int offset) {
        long v = Double.doubleToLongBits(val);
        longBytesBE(v, buffer, offset);
    }
    
    public static double doubleValueLE(byte[] bytes) {
        long bits = longValueLE(bytes);
        return Double.longBitsToDouble(bits);
    }
    
    public static double doubleValueBE(byte[] bytes) {
        long bits = longValueBE(bytes);
        return Double.longBitsToDouble(bits);
    }
    
    public static double doubleValueLE(byte[] bytes, int offset) {
        long bits = longValueLE(bytes, offset);
        return Double.longBitsToDouble(bits);
    }
    
    public static double doubleValueBE(byte[] bytes, int offset) {
        long bits = longValueBE(bytes, offset);
        return Double.longBitsToDouble(bits);
    }
}
