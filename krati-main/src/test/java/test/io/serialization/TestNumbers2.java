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

package test.io.serialization;

import java.util.Random;

import junit.framework.TestCase;
import krati.util.Numbers;

/**
 * TestNumbers2
 * 
 * @author jwu
 * @since 04/05, 2012
 */
public class TestNumbers2 extends TestCase {
    Random _rand = new Random();
    
    public void testIntLE() {
        int num1, num2;
        byte[] bytes = new byte[7];
        
        num1 = 0;
        Numbers.intBytesLE(num1, bytes);
        num2 = Numbers.intValueLE(bytes);
        assertEquals(num1, num2);
        
        num1 = 1;
        Numbers.intBytesLE(num1, bytes);
        num2 = Numbers.intValueLE(bytes);
        assertEquals(num1, num2);
        
        num1 = -1;
        Numbers.intBytesLE(num1, bytes);
        num2 = Numbers.intValueLE(bytes);
        assertEquals(num1, num2);
        
        num1 = Integer.MAX_VALUE;
        Numbers.intBytesLE(num1, bytes);
        num2 = Numbers.intValueLE(bytes);
        assertEquals(num1, num2);
        
        num1 = Integer.MIN_VALUE;
        Numbers.intBytesLE(num1, bytes);
        num2 = Numbers.intValueLE(bytes);
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = _rand.nextInt();
            Numbers.intBytesLE(num1, bytes);
            num2 = Numbers.intValueLE(bytes);
            assertEquals(num1, num2);
        }
    }
    
    public void testIntBE() {
        int num1, num2;
        byte[] bytes = new byte[9];
        
        num1 = 0;
        Numbers.intBytesBE(num1, bytes);
        num2 = Numbers.intValueBE(bytes);
        assertEquals(num1, num2);
        
        num1 = 1;
        Numbers.intBytesBE(num1, bytes);
        num2 = Numbers.intValueBE(bytes);
        assertEquals(num1, num2);
        
        num1 = -1;
        Numbers.intBytesBE(num1, bytes);
        num2 = Numbers.intValueBE(bytes);
        assertEquals(num1, num2);
        
        num1 = Integer.MAX_VALUE;
        Numbers.intBytesBE(num1, bytes);
        num2 = Numbers.intValueBE(bytes);
        assertEquals(num1, num2);
        
        num1 = Integer.MIN_VALUE;
        Numbers.intBytesBE(num1, bytes);
        num2 = Numbers.intValueBE(bytes);
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = _rand.nextInt();
            Numbers.intBytesBE(num1, bytes);
            num2 = Numbers.intValueBE(bytes);
            assertEquals(num1, num2);
        }
    }
    
    public void testLongLE() {
        long num1, num2;
        byte[] bytes = new byte[10];
        
        num1 = 0L;
        Numbers.longBytesLE(num1, bytes);
        num2 = Numbers.longValueLE(bytes);
        assertEquals(num1, num2);
        
        num1 = 1L;
        Numbers.longBytesLE(num1, bytes);
        num2 = Numbers.longValueLE(bytes);
        assertEquals(num1, num2);
        
        num1 = -1L;
        Numbers.longBytesLE(num1, bytes);
        num2 = Numbers.longValueLE(bytes);
        assertEquals(num1, num2);
        
        num1 = Long.MAX_VALUE;
        Numbers.longBytesLE(num1, bytes);
        num2 = Numbers.longValueLE(bytes);
        assertEquals(num1, num2);
        
        num1 = Long.MIN_VALUE;
        Numbers.longBytesLE(num1, bytes);
        num2 = Numbers.longValueLE(bytes);
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = _rand.nextLong();
            Numbers.longBytesLE(num1, bytes);
            num2 = Numbers.longValueLE(bytes);
            assertEquals(num1, num2);
        }
    }
    
    public void testLongBE() {
        long num1, num2;
        byte[] bytes = new byte[12]; 
        
        num1 = 0L;
        Numbers.longBytesBE(num1, bytes);
        num2 = Numbers.longValueBE(bytes);
        assertEquals(num1, num2);
        
        num1 = 1L;
        Numbers.longBytesBE(num1, bytes);
        num2 = Numbers.longValueBE(bytes);
        assertEquals(num1, num2);
        
        num1 = -1L;
        Numbers.longBytesBE(num1, bytes);
        num2 = Numbers.longValueBE(bytes);
        assertEquals(num1, num2);
        
        num1 = Long.MAX_VALUE;
        Numbers.longBytesBE(num1, bytes);
        num2 = Numbers.longValueBE(bytes);
        assertEquals(num1, num2);
        
        num1 = Long.MIN_VALUE;
        Numbers.longBytesBE(num1, bytes);
        num2 = Numbers.longValueBE(bytes);
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = _rand.nextLong();
            Numbers.longBytesBE(num1, bytes);
            num2 = Numbers.longValueBE(bytes);
            assertEquals(num1, num2);
        }
    }
    public void testShortLE() {
        short num1, num2;
        byte[] bytes = new byte[7];
        
        num1 = 0;
        Numbers.shortBytesLE(num1, bytes);
        num2 = Numbers.shortValueLE(bytes);
        assertEquals(num1, num2);
        
        num1 = 1;
        Numbers.shortBytesLE(num1, bytes);
        num2 = Numbers.shortValueLE(bytes);
        assertEquals(num1, num2);
        
        num1 = -1;
        Numbers.shortBytesLE(num1, bytes);
        num2 = Numbers.shortValueLE(bytes);
        assertEquals(num1, num2);
        
        num1 = Short.MAX_VALUE;
        Numbers.shortBytesLE(num1, bytes);
        num2 = Numbers.shortValueLE(bytes);
        assertEquals(num1, num2);
        
        num1 = Short.MIN_VALUE;
        Numbers.shortBytesLE(num1, bytes);
        num2 = Numbers.shortValueLE(bytes);
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = (short)_rand.nextInt();
            Numbers.shortBytesLE(num1, bytes);
            num2 = Numbers.shortValueLE(bytes);
            assertEquals(num1, num2);
        }
    }
    
    public void testShortBE() {
        short num1, num2;
        byte[] bytes = new byte[9];
        
        num1 = 0;
        Numbers.shortBytesBE(num1, bytes);
        num2 = Numbers.shortValueBE(bytes);
        assertEquals(num1, num2);
        
        num1 = 1;
        Numbers.shortBytesBE(num1, bytes);
        num2 = Numbers.shortValueBE(bytes);
        assertEquals(num1, num2);
        
        num1 = -1;
        Numbers.shortBytesBE(num1, bytes);
        num2 = Numbers.shortValueBE(bytes);
        assertEquals(num1, num2);
        
        num1 = Short.MAX_VALUE;
        Numbers.shortBytesBE(num1, bytes);
        num2 = Numbers.shortValueBE(bytes);
        assertEquals(num1, num2);
        
        num1 = Short.MIN_VALUE;
        Numbers.shortBytesBE(num1, bytes);
        num2 = Numbers.shortValueBE(bytes);
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = (short)_rand.nextInt();
            Numbers.shortBytesBE(num1, bytes);
            num2 = Numbers.shortValueBE(bytes);
            assertEquals(num1, num2);
        }
    }
}
