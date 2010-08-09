package test.cds;

import krati.cds.impl.array.basic.DynamicConstants;
import krati.cds.impl.array.basic.DynamicLongArrayDual;

import org.junit.Test;

import test.AbstractTest;

public class TestDynamicArrayDual extends AbstractTest
{
    int maxEntrySize = 1000;
    int maxEntries = 5;
    
    public TestDynamicArrayDual()
    {
        super(TestDynamicArrayDual.class.getSimpleName());
    }
    
    @Test
    public void testDynamicLongArrayDual() throws Exception
    {
        cleanTestOutput();
        
        // Create the first long array and do random updates
        DynamicLongArrayDual array1 = new DynamicLongArrayDual(maxEntrySize, maxEntries, getHomeDirectory());
        
        int memberIdStart = 0;
        MemberDataUpdate[] updates1 = MemberDataUpdate.generateUpdates(memberIdStart, DynamicConstants.SUB_ARRAY_SIZE);
        memberIdStart += DynamicConstants.SUB_ARRAY_SIZE;
        MemberDataUpdate[] updates2 = MemberDataUpdate.generateUpdates(memberIdStart, DynamicConstants.SUB_ARRAY_SIZE);
        memberIdStart += DynamicConstants.SUB_ARRAY_SIZE;
        MemberDataUpdate[] updates3 = MemberDataUpdate.generateUpdates(memberIdStart, DynamicConstants.SUB_ARRAY_SIZE);
        
        // 1st batch of updates
        for(MemberDataUpdate u : updates1) 
        {
            array1.set(u.getMemberId(), u.getData(), u.getData(), u.getScn());
        }
        
        int testIndex = 0;
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += DynamicConstants.SUB_ARRAY_SIZE;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == DynamicConstants.SUB_ARRAY_SIZE);
        
        // 2nd batch of updates
        for(MemberDataUpdate u : updates2) 
        {
            array1.set(u.getMemberId(), u.getData(), u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += DynamicConstants.SUB_ARRAY_SIZE;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == DynamicConstants.SUB_ARRAY_SIZE * 2);
        
        // 3rd batch of updates
        for(MemberDataUpdate u : updates3) 
        {
            array1.set(u.getMemberId(), u.getData(), u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += DynamicConstants.SUB_ARRAY_SIZE;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == DynamicConstants.SUB_ARRAY_SIZE * 3);
        
        // Random-update the entire array
        MemberDataUpdate[] updates = MemberDataUpdate.generateUpdates(0, array1.length());

        for(MemberDataUpdate u : updates) 
        {
            array1.set(u.getMemberId(), u.getData(), u.getData(), u.getScn());
        }
        
        for(MemberDataUpdate u : updates) 
        {
            assertEquals(array1.get(u.getMemberId()), u.getData());
        }
        
        // Create the second array, which should load data from cache
        DynamicLongArrayDual array2 = new DynamicLongArrayDual(maxEntrySize, maxEntries, getHomeDirectory());
        assertTrue("array2.getHWMark() is greater than array1.getHWMark()", array2.getHWMark() <= array1.getHWMark());
        
        // Persist the first array
        array1.persist();
        
        // Create the third array, which should load data from cache
        DynamicLongArrayDual array3 = new DynamicLongArrayDual(maxEntrySize, maxEntries, getHomeDirectory());
        assertTrue("array3.getHWMark() is greater than array1.getHWMark()", array3.getHWMark() <= array1.getHWMark());

        // Compare array1 and array3
        int nonZeroCount = 0;
        for(int index = 0; index < array1.length(); index++)
        {
            if (array1.get(index) > 0) nonZeroCount++;
            assertEquals(array1.get(index), array3.get(index));
            assertEquals(array1.get(index), array1.getDual(index));
            assertEquals(array1.getDual(index), array3.getDual(index));
        }
        
        assertTrue("all zeros in array1", nonZeroCount > 0);
        
        cleanTestOutput();
    }
}
