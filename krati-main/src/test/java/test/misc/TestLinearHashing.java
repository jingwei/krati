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

package test.misc;

import java.util.Random;

import junit.framework.TestCase;
import krati.core.array.basic.DynamicConstants;
import krati.util.LinearHashing;

/**
 * TestLinearHashing
 * 
 * @author jwu
 * 06/08, 2011
 * 
 */
public class TestLinearHashing extends TestCase {
    
    public void testBasics() {
        int capacity;
        int unitCapacity = DynamicConstants.SUB_ARRAY_SIZE;
        LinearHashing h = new LinearHashing(unitCapacity);
        assertEquals(unitCapacity, h.getUnitCapacity());
        
        assertEquals(0, h.getSplit());
        assertEquals(0, h.getLevel());
        assertEquals(unitCapacity, h.getLevelCapacity());
        
        capacity = unitCapacity;
        assertEquals(0, h.getSplit());
        assertEquals(0, h.getLevel());
        assertEquals(unitCapacity, h.getLevelCapacity());
        
        capacity = unitCapacity << 1;
        h.reinit(capacity);
        assertEquals(0, h.getSplit());
        assertEquals(0, h.getLevel());
        assertEquals(unitCapacity, h.getLevelCapacity());
        
        capacity = unitCapacity << 2;
        h.reinit(capacity);
        assertEquals(unitCapacity, h.getSplit());
        assertEquals(1, h.getLevel());
        assertEquals(unitCapacity << 1, h.getLevelCapacity());
        
        capacity = unitCapacity << 3;
        h.reinit(capacity);
        assertEquals((unitCapacity << 2) - unitCapacity, h.getSplit());
        assertEquals(2, h.getLevel());
        assertEquals(unitCapacity << 2, h.getLevelCapacity());
        
        capacity = (unitCapacity << 3) - 1;
        h.reinit(capacity);
        assertEquals((unitCapacity << 2) - 2 * unitCapacity, h.getSplit());
        assertEquals(2, h.getLevel());
        assertEquals(unitCapacity << 2, h.getLevelCapacity());

        capacity = (unitCapacity << 3) - unitCapacity + 1;
        h.reinit(capacity);
        assertEquals((unitCapacity << 2) - 2 * unitCapacity, h.getSplit());
        assertEquals(2, h.getLevel());
        assertEquals(unitCapacity << 2, h.getLevelCapacity());
        
        capacity = (unitCapacity << 3) - unitCapacity;
        h.reinit(capacity);
        assertEquals((unitCapacity << 2) - 2 * unitCapacity, h.getSplit());
        assertEquals(2, h.getLevel());
        assertEquals(unitCapacity << 2, h.getLevelCapacity());
        
        capacity = Integer.MAX_VALUE;
        h.reinit(capacity);
        assertEquals((1 << 30) - (unitCapacity << 1), h.getSplit());
        assertEquals(14, h.getLevel());
        assertEquals(1 << 30, h.getLevelCapacity());
    }
    
    public void testRandom() {
        int unitCapacity = 1 << DynamicConstants.SUB_ARRAY_BITS;
        LinearHashing h = new LinearHashing(unitCapacity);
        assertEquals(unitCapacity, h.getUnitCapacity());
        
        for(int level = 0; level < 11; level++) {
            check(h, level);
        }
    }
    
    private void check(LinearHashing h, int level) {
        int capacity;
        int levelCapacity = h.getUnitCapacity() << level;
        int nextLevelCapacity = h.getUnitCapacity() << (level + 1);
        Random rand = new Random();
        
        if(nextLevelCapacity > levelCapacity) {
            capacity = nextLevelCapacity - rand.nextInt(h.getUnitCapacity());
            h.reinit(capacity);
            assertEquals(Math.max(0, levelCapacity - 2 * h.getUnitCapacity()), h.getSplit());
            assertEquals(level, h.getLevel());
            assertEquals(h.getUnitCapacity() << level, h.getLevelCapacity());
            
            capacity = nextLevelCapacity - h.getUnitCapacity() + rand.nextInt(h.getUnitCapacity());
            h.reinit(capacity);
            assertEquals(Math.max(0, levelCapacity - 2 * h.getUnitCapacity()), h.getSplit());
            assertEquals(level, h.getLevel());
            assertEquals(h.getUnitCapacity() << level, h.getLevelCapacity());
            
            capacity = nextLevelCapacity - h.getUnitCapacity();
            h.reinit(capacity);
            assertEquals(Math.max(0, levelCapacity - 2 * h.getUnitCapacity()), h.getSplit());
            assertEquals(level, h.getLevel());
            assertEquals(h.getUnitCapacity() << level, h.getLevelCapacity());
            
            capacity = nextLevelCapacity;
            h.reinit(capacity);
            assertEquals(Math.max(0, levelCapacity - h.getUnitCapacity()), h.getSplit());
            assertEquals(level, h.getLevel());
            assertEquals(h.getUnitCapacity() << level, h.getLevelCapacity());
        }
    }
    
    public void testMaxLevel() {
        int unitCapacity = DynamicConstants.SUB_ARRAY_SIZE;
        LinearHashing h = new LinearHashing(unitCapacity);
        h.reinit(Integer.MAX_VALUE);
        assertEquals(14, h.getLevel());
        assertEquals(1 << 30, h.getLevelCapacity());
    }
}
