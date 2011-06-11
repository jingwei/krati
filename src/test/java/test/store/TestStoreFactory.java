package test.store;

import java.util.Random;

import junit.framework.TestCase;

import krati.core.StoreParams;
import krati.core.array.basic.DynamicConstants;

/**
 * TestStoreFactory
 * 
 * @author jwu
 * 06/09, 2011
 * 
 */
public class TestStoreFactory extends TestCase {
    Random _rand = new Random();
    
    public void testInitLevel() {
        int unitCapacity = DynamicConstants.SUB_ARRAY_SIZE;
        int initialCapacity;
        
        initialCapacity = unitCapacity;
        assertEquals(0, StoreParams.getDynamicStoreInitialLevel(initialCapacity));
        
        initialCapacity = unitCapacity + unitCapacity / 2;
        assertEquals(1, StoreParams.getDynamicStoreInitialLevel(initialCapacity));
        
        initialCapacity = unitCapacity << 1;
        assertEquals(1, StoreParams.getDynamicStoreInitialLevel(initialCapacity));
        
        for(int i = 0, cnt = 10; i < cnt; i++) {
            int level = _rand.nextInt(14);
            
            initialCapacity = unitCapacity << level;
            assertEquals(level, StoreParams.getDynamicStoreInitialLevel(initialCapacity));
            
            initialCapacity = (unitCapacity << level) + 1;
            assertEquals(level + 1, StoreParams.getDynamicStoreInitialLevel(initialCapacity));
            
            initialCapacity = (unitCapacity << level) + (unitCapacity / 2);
            assertEquals(level + 1, StoreParams.getDynamicStoreInitialLevel(initialCapacity));
            
            initialCapacity = (unitCapacity << (level + 1)) - 1;
            assertEquals(level + 1, StoreParams.getDynamicStoreInitialLevel(initialCapacity));
        }
        
        initialCapacity = Integer.MAX_VALUE;
        assertEquals(15, StoreParams.getDynamicStoreInitialLevel(initialCapacity));
    }
}
