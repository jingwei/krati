package test.io.serialization;

import java.util.Random;

import junit.framework.TestCase;
import krati.util.Numbers;

/**
 * TestNumbers3
 * 
 * @author jwu
 * @since 04/05, 2012
 */
public class TestNumbers3 extends TestCase {
    Random _rand = new Random();
    
    public void testIntLE() {
        intLE(0);
        intLE(1);
        intLE(2);
        intLE(3);
    }
    
    void intLE(int offset) {
        int num1, num2;
        byte[] bytes = new byte[7];
        
        num1 = 0;
        Numbers.intBytesLE(num1, bytes, offset);
        num2 = Numbers.intValueLE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = 1;
        Numbers.intBytesLE(num1, bytes, offset);
        num2 = Numbers.intValueLE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = -1;
        Numbers.intBytesLE(num1, bytes, offset);
        num2 = Numbers.intValueLE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = Integer.MAX_VALUE;
        Numbers.intBytesLE(num1, bytes, offset);
        num2 = Numbers.intValueLE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = Integer.MIN_VALUE;
        Numbers.intBytesLE(num1, bytes, offset);
        num2 = Numbers.intValueLE(bytes, offset);
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = _rand.nextInt();
            Numbers.intBytesLE(num1, bytes, offset);
            num2 = Numbers.intValueLE(bytes, offset);
            assertEquals(num1, num2);
        }
    }
    
    public void testIntBE() {
        intBE(0);
        intBE(1);
        intBE(2);
        intBE(3);
        intBE(4);
        intBE(5);
    }
    
    void intBE(int offset) {
        int num1, num2;
        byte[] bytes = new byte[9];
        
        num1 = 0;
        Numbers.intBytesBE(num1, bytes, offset);
        num2 = Numbers.intValueBE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = 1;
        Numbers.intBytesBE(num1, bytes, offset);
        num2 = Numbers.intValueBE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = -1;
        Numbers.intBytesBE(num1, bytes, offset);
        num2 = Numbers.intValueBE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = Integer.MAX_VALUE;
        Numbers.intBytesBE(num1, bytes, offset);
        num2 = Numbers.intValueBE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = Integer.MIN_VALUE;
        Numbers.intBytesBE(num1, bytes, offset);
        num2 = Numbers.intValueBE(bytes, offset);
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = _rand.nextInt();
            Numbers.intBytesBE(num1, bytes, offset);
            num2 = Numbers.intValueBE(bytes, offset);
            assertEquals(num1, num2);
        }
    }
    
    public void testLongLE() {
        longLE(0);
        longLE(1);
        longLE(2);
    }
    
    void longLE(int offset) {
        long num1, num2;
        byte[] bytes = new byte[10];
        
        num1 = 0L;
        Numbers.longBytesLE(num1, bytes, offset);
        num2 = Numbers.longValueLE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = 1L;
        Numbers.longBytesLE(num1, bytes, offset);
        num2 = Numbers.longValueLE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = -1L;
        Numbers.longBytesLE(num1, bytes, offset);
        num2 = Numbers.longValueLE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = Long.MAX_VALUE;
        Numbers.longBytesLE(num1, bytes, offset);
        num2 = Numbers.longValueLE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = Long.MIN_VALUE;
        Numbers.longBytesLE(num1, bytes, offset);
        num2 = Numbers.longValueLE(bytes, offset);
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = _rand.nextLong();
            Numbers.longBytesLE(num1, bytes, offset);
            num2 = Numbers.longValueLE(bytes, offset);
            assertEquals(num1, num2);
        }
    }
    
    public void testLongBE() {
        longBE(0);
        longBE(1);
        longBE(2);
        longBE(3);
        longBE(4);
    }
    
    void longBE(int offset) {
        long num1, num2;
        byte[] bytes = new byte[12]; 
        
        num1 = 0L;
        Numbers.longBytesBE(num1, bytes, offset);
        num2 = Numbers.longValueBE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = 1L;
        Numbers.longBytesBE(num1, bytes, offset);
        num2 = Numbers.longValueBE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = -1L;
        Numbers.longBytesBE(num1, bytes, offset);
        num2 = Numbers.longValueBE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = Long.MAX_VALUE;
        Numbers.longBytesBE(num1, bytes, offset);
        num2 = Numbers.longValueBE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = Long.MIN_VALUE;
        Numbers.longBytesBE(num1, bytes, offset);
        num2 = Numbers.longValueBE(bytes, offset);
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = _rand.nextLong();
            Numbers.longBytesBE(num1, bytes, offset);
            num2 = Numbers.longValueBE(bytes, offset);
            assertEquals(num1, num2);
        }
    }
    public void testShortLE() {
        shortLE(0);
        shortLE(1);
        shortLE(2);
        shortLE(3);
    }
    
    void shortLE(int offset) {
        short num1, num2;
        byte[] bytes = new byte[7];
        
        num1 = 0;
        Numbers.shortBytesLE(num1, bytes, offset);
        num2 = Numbers.shortValueLE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = 1;
        Numbers.shortBytesLE(num1, bytes, offset);
        num2 = Numbers.shortValueLE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = -1;
        Numbers.shortBytesLE(num1, bytes, offset);
        num2 = Numbers.shortValueLE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = Short.MAX_VALUE;
        Numbers.shortBytesLE(num1, bytes, offset);
        num2 = Numbers.shortValueLE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = Short.MIN_VALUE;
        Numbers.shortBytesLE(num1, bytes, offset);
        num2 = Numbers.shortValueLE(bytes, offset);
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = (short)_rand.nextInt();
            Numbers.shortBytesLE(num1, bytes, offset);
            num2 = Numbers.shortValueLE(bytes, offset);
            assertEquals(num1, num2);
        }
    }
    
    public void testShortBE() {
        shortBE(0);
        shortBE(1);
        shortBE(2);
        shortBE(3);
        shortBE(4);
        shortBE(5);
    }
    
    void shortBE(int offset) {
        short num1, num2;
        byte[] bytes = new byte[9];
        
        num1 = 0;
        Numbers.shortBytesBE(num1, bytes, offset);
        num2 = Numbers.shortValueBE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = 1;
        Numbers.shortBytesBE(num1, bytes, offset);
        num2 = Numbers.shortValueBE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = -1;
        Numbers.shortBytesBE(num1, bytes, offset);
        num2 = Numbers.shortValueBE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = Short.MAX_VALUE;
        Numbers.shortBytesBE(num1, bytes, offset);
        num2 = Numbers.shortValueBE(bytes, offset);
        assertEquals(num1, num2);
        
        num1 = Short.MIN_VALUE;
        Numbers.shortBytesBE(num1, bytes, offset);
        num2 = Numbers.shortValueBE(bytes, offset);
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = (short)_rand.nextInt();
            Numbers.shortBytesBE(num1, bytes, offset);
            num2 = Numbers.shortValueBE(bytes, offset);
            assertEquals(num1, num2);
        }
    }
}
