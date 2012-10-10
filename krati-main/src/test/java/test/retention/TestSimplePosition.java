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
import krati.retention.Position;

public class TestSimplePosition extends TestCase {
    
    public void testStringSerialization() {
        Position p1 = new SimplePosition(10, 5, 3, new Clock(11,17,23));
        Position p2 = new SimplePosition(10, 5, 3, new Clock(11,17,23));
        Position p3 = new SimplePosition(2, 4, 6, new Clock(8,10,12));
        assertEquals(p1, p2);
        assertFalse(p1.equals(p3));
    }
}
