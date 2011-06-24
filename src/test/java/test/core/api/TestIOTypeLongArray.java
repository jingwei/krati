package test.core.api;

import java.io.File;

import krati.array.Array;
import krati.core.array.AddressArray;
import krati.core.array.basic.DynamicConstants;
import krati.core.array.basic.IOTypeLongArray;

/**
 * TestIOTypeLongArray
 * 
 * @author jwu
 * 06/21, 2011
 * 
 */
public class TestIOTypeLongArray extends AbstractTestDynamicAddressArray {
    
    @Override
    protected AddressArray createAddressArray(File homeDir) throws Exception {
        return new IOTypeLongArray(Array.Type.DYNAMIC, DynamicConstants.SUB_ARRAY_SIZE, getBatchSize(), getNumSyncBatches(), homeDir);
    }
    
    public void testCapacity() throws Exception {
        int index;
        int length;
        
        length = _array.length();
        index = length + _rand.nextInt(length);
        _array.expandCapacity(index);
        assertTrue(length < _array.length());
        
        length = _array.length();
        index = length + _rand.nextInt(length);
        _array.expandCapacity(index);
        assertTrue(length < _array.length());
        
        length = _array.length();
        index = length + _rand.nextInt(length);
        _array.expandCapacity(index);
        assertTrue(length < _array.length());
        
        index = Integer.MAX_VALUE;
        _array.expandCapacity(index);
        assertEquals(Integer.MAX_VALUE, _array.length());
    }
}
