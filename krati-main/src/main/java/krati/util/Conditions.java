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

import krati.store.InvalidDataException;

/**
 * Conditions
 * 
 * @author jwu
 * @since 08/22, 2012
 */
public class Conditions {
    /**
     * Tests the specified <code>object</code> is not null.
     */
    public static void notNull(Object object) {
       if(object == null) {
           throw new NullPointerException();
       }
    }
    
    /**
     * Tests the specified <code>object</code> is not null.
     */
    public static void notNull(Object object, String message) {
       if(object == null) {
           throw new NullPointerException(message);
       }
    }
    
    /**
     * Tests the specified <code>size</code> is equal to the specified <code>expected</code>.
     * 
     * @param size     - the size
     * @param expected - the expected size
     */
    public static void checkSize(int size, int expected) {
        if(size != expected) {
            throw new InvalidDataException("Invalid size: " + size  + " expected: " + expected);
        }
    }
    
    /**
     * Tests the specified <code>keySize</code> is equal to the specified <code>expected</code>.
     * 
     * @param keySize  - the key size
     * @param expected - the expected size
     */
    public static void checkKeySize(int keySize, int expected) {
        if(keySize != expected) {
            throw new InvalidDataException("Invalid key size: " + keySize  + " expected: " + expected);
        }
    }
    
    /**
     * Tests the specified <code>valueSize</code> is equal to the specified <code>expected</code>.
     * 
     * @param valueSize - the value size
     * @param expected  - the expected size
     */
    public static void checkValueSize(int valueSize, int expected) {
        if(valueSize != expected) {
            throw new InvalidDataException("Invalid value size: " + valueSize  + " expected: " + expected);
        }
    }
    
    /**
     * Tests the specified <code>dataSize</code> is equal to the specified <code>expected</code>.
     * 
     * @param dataSize - the data size
     * @param expected - the expected size
     */
    public static void checkDataSize(int dataSize, int expected) {
        if(dataSize != expected) {
            throw new InvalidDataException("Invalid data size: " + dataSize  + " expected: " + expected);
        }
    }
    
    /**
     * Tests the specified <code>size</code> is less than or equal to the specified <code>maxSize</code>.
     * 
     * @param size    - the size
     * @param maxSize - the max size
     */
    public static void checkMaxSize(int size, int maxSize) {
        if(size < 0 || size > maxSize) {
            throw new InvalidDataException("Invalid size: " + size  + " max: " + maxSize);
        }
    }
    
    /**
     * Tests the specified <code>keySize</code> is less than or equal to the specified <code>maxKeySize</code>.
     * 
     * @param keySize    - the key size
     * @param maxKeySize - the max key size
     */
    public static void checkMaxKeySize(int keySize, int maxKeySize) {
        if(keySize < 0 || keySize > maxKeySize) {
            throw new InvalidDataException("Invalid key size: " + keySize  + " max: " + maxKeySize);
        }
    }
    
    /**
     * Tests the specified <code>valueSize</code> is less than or equal to the specified <code>maxValueSize</code>.
     * 
     * @param valueSize    - the value size
     * @param maxValueSize - the max value size
     */
    public static void checkMaxValueSize(int valueSize, int maxValueSize) {
        if(valueSize < 0 || valueSize > maxValueSize) {
            throw new InvalidDataException("Invalid value size: " + valueSize  + " max: " + maxValueSize);
        }
    }
    
    /**
     * Tests the specified <code>dataSize</code> is less than or equal to the specified <code>maxDataSize</code>.
     * 
     * @param dataSize    - the data size
     * @param maxDataSize - the max data size
     */
    public static void checkMaxDataSize(int dataSize, int maxDataSize) {
        if(dataSize < 0 || dataSize > maxDataSize) {
            throw new InvalidDataException("Invalid data size: " + dataSize  + " max: " + maxDataSize);
        }
    }
}
