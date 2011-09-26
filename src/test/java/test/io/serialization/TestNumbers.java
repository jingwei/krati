package test.io.serialization;

import java.util.Random;

import junit.framework.TestCase;
import krati.util.Numbers;

/**
 * TestNumbers
 * 
 * @author jwu
 * @since 09/25, 2011
 */
public class TestNumbers extends TestCase {
    Random _rand = new Random();
    
    public void testIntLE() {
        int num1, num2;
        
        num1 = 0;
        num2 = Numbers.intValueLE(Numbers.intBytesLE(num1));
        assertEquals(num1, num2);
        
        num1 = 1;
        num2 = Numbers.intValueLE(Numbers.intBytesLE(num1));
        assertEquals(num1, num2);
        
        num1 = -1;
        num2 = Numbers.intValueLE(Numbers.intBytesLE(num1));
        assertEquals(num1, num2);
        
        num1 = Integer.MAX_VALUE;
        num2 = Numbers.intValueLE(Numbers.intBytesLE(num1));
        assertEquals(num1, num2);
        
        num1 = Integer.MIN_VALUE;
        num2 = Numbers.intValueLE(Numbers.intBytesLE(num1));
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = _rand.nextInt();
            num2 = Numbers.intValueLE(Numbers.intBytesLE(num1));
            assertEquals(num1, num2);
        }
    }
    
    public void testIntBE() {
        int num1, num2;
        
        num1 = 0;
        num2 = Numbers.intValueBE(Numbers.intBytesBE(num1));
        assertEquals(num1, num2);
        
        num1 = 1;
        num2 = Numbers.intValueBE(Numbers.intBytesBE(num1));
        assertEquals(num1, num2);
        
        num1 = -1;
        num2 = Numbers.intValueBE(Numbers.intBytesBE(num1));
        assertEquals(num1, num2);
        
        num1 = Integer.MAX_VALUE;
        num2 = Numbers.intValueBE(Numbers.intBytesBE(num1));
        assertEquals(num1, num2);
        
        num1 = Integer.MIN_VALUE;
        num2 = Numbers.intValueBE(Numbers.intBytesBE(num1));
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = _rand.nextInt();
            num2 = Numbers.intValueBE(Numbers.intBytesBE(num1));
            assertEquals(num1, num2);
        }
    }
    
    public void testLongLE() {
        long num1, num2;
        
        num1 = 0L;
        num2 = Numbers.longValueLE(Numbers.longBytesLE(num1));
        assertEquals(num1, num2);
        
        num1 = 1L;
        num2 = Numbers.longValueLE(Numbers.longBytesLE(num1));
        assertEquals(num1, num2);
        
        num1 = -1L;
        num2 = Numbers.longValueLE(Numbers.longBytesLE(num1));
        assertEquals(num1, num2);
        
        num1 = Long.MAX_VALUE;
        num2 = Numbers.longValueLE(Numbers.longBytesLE(num1));
        assertEquals(num1, num2);
        
        num1 = Long.MIN_VALUE;
        num2 = Numbers.longValueLE(Numbers.longBytesLE(num1));
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = _rand.nextLong();
            num2 = Numbers.longValueLE(Numbers.longBytesLE(num1));
            assertEquals(num1, num2);
        }
    }
    
    public void testLongBE() {
        long num1, num2;
        
        num1 = 0L;
        num2 = Numbers.longValueBE(Numbers.longBytesBE(num1));
        assertEquals(num1, num2);
        
        num1 = 1L;
        num2 = Numbers.longValueBE(Numbers.longBytesBE(num1));
        assertEquals(num1, num2);
        
        num1 = -1L;
        num2 = Numbers.longValueBE(Numbers.longBytesBE(num1));
        assertEquals(num1, num2);
        
        num1 = Long.MAX_VALUE;
        num2 = Numbers.longValueBE(Numbers.longBytesBE(num1));
        assertEquals(num1, num2);
        
        num1 = Long.MIN_VALUE;
        num2 = Numbers.longValueBE(Numbers.longBytesBE(num1));
        assertEquals(num1, num2);
        
        for(int i = 0; i < 1000; i++) {
            num1 = _rand.nextLong();
            num2 = Numbers.longValueBE(Numbers.longBytesBE(num1));
            assertEquals(num1, num2);
        }
    }
}
