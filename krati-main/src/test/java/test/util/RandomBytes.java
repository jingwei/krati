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

package test.util;

import java.util.Random;

/**
 * RandomBytes
 * 
 * @author jwu
 * 06/01, 2011
 * 
 */
public class RandomBytes {
    private final static Random _rand = new Random();
    
    /**
     * @return a random byte array of size from 0 byte to 4096 bytes. 
     */
    public static byte[] getBytes() {
        byte[] bytes = new byte[_rand.nextInt(4096)];
        _rand.nextBytes(bytes);
        return bytes;
    }
    
    /**
     * Gets a fixed-length random byte array.
     * 
     * @param length - byte array length
     * @return a fixed-length random byte array.
     */
    public static byte[] getBytes(int length) {
        byte[] bytes = new byte[length];
        _rand.nextBytes(bytes);
        return bytes;
    }
}
