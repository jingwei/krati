package test.retention.clock;

import test.retention.util.RandomClockFactory;
import junit.framework.TestCase;
import krati.retention.clock.Clock;
import krati.retention.clock.ClockSerializer;
import krati.retention.clock.IncomparableClocksException;
import krati.retention.clock.Occurred;

/**
 * TestClock
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/11, 2011 - Created
 */
public class TestClock extends TestCase {
    
    public void testApiBasics() {
        RandomClockFactory f = new RandomClockFactory(3);
        
        Clock c1 = f.next();
        Clock c2 = f.next();
        assertTrue(c1.before(c2));
        assertTrue(c2.after(c1));
        
        long[] scnValues = (long[])c1.values().clone();
        Clock c = new Clock(scnValues);
        assertTrue(c.compareTo(c1) == Occurred.EQUICONCURRENTLY);
        assertTrue(c1.compareTo(c) == Occurred.EQUICONCURRENTLY);
        
        scnValues[0] = scnValues[0] + 1;
        c = new Clock(scnValues);
        assertTrue(c.after(c1));
        assertTrue(c1.before(c));
        
        scnValues[1] = c.values()[1] - 1;
        c = new Clock(scnValues);
        
        assertEquals(Occurred.CONCURRENTLY, c.compareTo(c1));
        
        long[] scnValuesNew = new long[2];
        scnValuesNew[0] = scnValuesNew[0];
        scnValuesNew[1] = scnValuesNew[1];
        c = new Clock(scnValuesNew);
        
        try {
            c.compareTo(c1);
            assertTrue(false);
        } catch(IncomparableClocksException e) {
            assertTrue(true);
        }
    }
    
    public void testClockSerializer() {
        ClockSerializer serializer = new ClockSerializer();
        RandomClockFactory f = new RandomClockFactory(5);
        Clock c = f.next();
        
        byte[] raw = c.toByteArray();
        Clock c2 = Clock.parseClock(raw);
        Clock c3 = serializer.deserialize(raw);
        
        assertEquals(Occurred.EQUICONCURRENTLY, c.compareTo(c2));
        assertEquals(Occurred.EQUICONCURRENTLY, c.compareTo(c3));
        
        String str = c.toString();
        Clock c4 = Clock.parseClock(str);
        assertEquals(Occurred.EQUICONCURRENTLY, c.compareTo(c4));
        
        assertTrue(c.after(Clock.ZERO));
    }
}
