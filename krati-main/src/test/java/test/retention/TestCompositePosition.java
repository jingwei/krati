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

package test.retention;


import junit.framework.TestCase;
import krati.retention.clock.Clock;
import krati.retention.SimplePosition;
import krati.retention.CompositePosition;
import krati.retention.Position;

public class TestCompositePosition extends TestCase {
    public void testClassInvariant() {
        Position p1 = new SimplePosition(10, 5, -1, new Clock(11,17,23));
        Position p2 = new SimplePosition(2, 4, 1000, new Clock(8,10,12));
        try {
            new CompositePosition(p1, p2);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }
    
    public void testEquals() {
        Position p1 = new SimplePosition(10, 5, -1, new Clock(11,17,23));
        Position p2 = new SimplePosition(2, 4, -1, new Clock(8,10,12));
        Position p3 = new SimplePosition(2, 4, -1, new Clock(8,10,12));
        CompositePosition c1 = new CompositePosition(p1, p2);
        CompositePosition c2 = new CompositePosition(p1, p3);
        CompositePosition c3 = new CompositePosition(p2, p3);
        assertEquals(c1, c2);
        assertFalse(c1.equals(c3));
        
    }
    public void testStringSerialization() {
        Position p1 = new SimplePosition(10, 5, -1, new Clock(11,17,23));
        Position p2 = new SimplePosition(2, 4, -1, new Clock(8,10,12));
        CompositePosition cp = new CompositePosition(p1, p2);
        String foo = cp.toString();
        CompositePosition cp2 = CompositePosition.parsePosition(foo);
        assertEquals(cp, cp2);        
    }
}
