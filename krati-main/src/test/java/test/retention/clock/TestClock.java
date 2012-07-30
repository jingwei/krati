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

package test.retention.clock;

import test.retention.util.RandomClockFactory;
import junit.framework.TestCase;
import krati.retention.SimplePosition;
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
    
    public void testEquals() {
        Clock c1 = new Clock(2, 12, 5);
        Clock c2 = new Clock(2, 12, 5);
        Clock c3 = new Clock(2, 12, 5, 0);
        assertEquals(c1, c2);
        assertFalse(c1.equals(c3));
    }
    
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
    
    public void testClockZero() {
        Clock c1 = Clock.parseClock(Clock.ZERO.toByteArray());
        assertEquals(Clock.ZERO, c1);
        
        Clock c2 = Clock.parseClock(Clock.ZERO.toString());
        assertEquals(Clock.ZERO, c2);
        
        SimplePosition position = new SimplePosition(1, 23467L, Clock.ZERO);
        Clock c3 = SimplePosition.parsePosition(position.toString()).getClock();
        assertEquals(Clock.ZERO, c3);
    }
}
