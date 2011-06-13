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
    
    public void testBasic() {
        int capacity;
        int unitCapacity = 1 << DynamicConstants.SUB_ARRAY_BITS;
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
}
