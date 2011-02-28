package test.misc;

import krati.core.segment.AddressFormat;
import test.AbstractTest;

/**
 * TestAddressFormat
 * 
 * @author jwu
 * 
 */
public class TestAddressFormat extends AbstractTest {

    public TestAddressFormat() {
        super(TestAddressFormat.class.getSimpleName());
    }
    
    public void testAddressFormat() {
        int offset, segment, dataSize;
        AddressFormat af = new AddressFormat();
        
        // Test 1
        offset = 12345;
        segment = 24;
        dataSize = (1 << 16) - 1;
        check(af, offset, segment, dataSize);
        
        // Test 2
        offset = 12345;
        segment = 24;
        dataSize = (1 << 16);
        check(af, offset, segment, dataSize);
        
        // Test 3
        offset = 12345;
        segment = 24;
        dataSize = 0;
        check(af, offset, segment, dataSize);
        
        // Test 4
        offset = Integer.MAX_VALUE;
        segment = 24;
        dataSize = 1023;
        check(af, offset, segment, dataSize);
        
        // Test 5
        offset = 0;
        segment = 24;
        dataSize = 1023;
        check(af, offset, segment, dataSize);
        
        // Test 6
        offset = 0;
        segment = 24;
        dataSize = af.getMaxDataSize();
        check(af, offset, segment, dataSize);
    }
    
    private void check(AddressFormat af, int offset, int segment, int dataSize) {
        long addr = af.composeAddress(offset, segment, dataSize);
        int offset2 = af.getOffset(addr);
        int segment2 = af.getSegment(addr);
        int dataSize2 = af.getDataSize(addr);
        
        assertEquals("offset", offset, offset2);
        assertEquals("segment", segment, segment2);
        assertEquals("dataSize", (dataSize > af.getMaxDataSize()) ? 0 : dataSize, dataSize2);
    }
}
