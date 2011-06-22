package test.core.api;

import java.io.File;

import krati.core.array.basic.IOTypeLongArray;

/**
 * TestIOTypeLongArray
 * 
 * @author jwu
 * 06/21, 2011
 * 
 */
public class TestIOTypeLongArray extends AbstractTestDynamicAddressArray<IOTypeLongArray> {
    
    @Override
    protected IOTypeLongArray createAddressArray(File homeDir) throws Exception {
        return new IOTypeLongArray(getBatchSize(), getNumSyncBatches(), homeDir);
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
