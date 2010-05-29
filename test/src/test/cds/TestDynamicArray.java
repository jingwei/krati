package test.cds;

import org.junit.Test;

import test.AbstractTest;

import java.util.Random;

import krati.cds.impl.array.DynamicIntArrayImpl;
import krati.cds.impl.array.DynamicLongArrayImpl;
import krati.cds.impl.array.DynamicShortArrayImpl;

/**
 * TestDynamicArray
 * 
 * @author jwu
 *
 */
public class TestDynamicArray extends AbstractTest
{
    static long scn = 0;
    static Random random = new Random(System.currentTimeMillis());
    
    int subArrayShift = 16;
    int subArraySize = 1 << subArrayShift;
    int maxEntrySize = 1000;
    int maxEntries = 10;
    
    public TestDynamicArray()
    {
        super(TestDynamicArray.class.getCanonicalName());
    }
    
    @Test
    public void testDynamicIntArray() throws Exception
    {
        cleanTestOutput();
        
        // Create the first long array and do random updates
        DynamicIntArrayImpl array1 = new DynamicIntArrayImpl(
                TEST_OUTPUT_DIR,
                subArrayShift,
                maxEntrySize,
                maxEntries);
        
        int memberIdStart = 0;
        MemberDataUpdate[] updates1 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        memberIdStart += subArraySize;
        MemberDataUpdate[] updates2 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        memberIdStart += subArraySize;
        MemberDataUpdate[] updates3 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        
        // 1st batch of updates
        for(MemberDataUpdate u : updates1) 
        {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        int testIndex = 0;
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize);
        
        // 2nd batch of updates
        for(MemberDataUpdate u : updates2) 
        {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 2);
        
        // 3rd batch of updates
        for(MemberDataUpdate u : updates3) 
        {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 3);
        
        // Random-update the entire array
        MemberDataUpdate[] updates = MemberDataUpdate.generateUpdates(0, array1.length());

        for(MemberDataUpdate u : updates) 
        {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        for(MemberDataUpdate u : updates) 
        {
            assert array1.get(u.getMemberId()) == u.getData();
        }
        
        // Create the second array, which should load data from cache
        DynamicIntArrayImpl array2 = new DynamicIntArrayImpl(
                TEST_OUTPUT_DIR,
                subArrayShift,
                maxEntrySize,
                maxEntries);
        assertTrue("array2.getHWMark() is greater than array1.getHWMark()", array2.getHWMark() <= array1.getHWMark());
        
        // Persist the first array
        array1.persist();
        
        // Create the third array, which should load data from cache
        DynamicIntArrayImpl array3 = new DynamicIntArrayImpl(
                TEST_OUTPUT_DIR,
                subArrayShift,
                maxEntrySize,
                maxEntries);
        assertTrue("array3.getHWMark() is greater than array1.getHWMark()", array3.getHWMark() <= array1.getHWMark());
        
        int nonZeroCount = 0;
        for(int index = 0; index < array1.length(); index++)
        {
            if (array1.get(index) > 0) nonZeroCount++;
            assert array1.get(index) == array3.get(index);
        }
        
        assertTrue("all zeros in array1", nonZeroCount > 0);
        
        cleanTestOutput();
    }
    
    @Test
    public void testDynamicLongArray() throws Exception
    {
        cleanTestOutput();
        
        // Create the first long array and do random updates
        DynamicLongArrayImpl array1 = new DynamicLongArrayImpl(
                TEST_OUTPUT_DIR,
                subArrayShift,
                maxEntrySize,
                maxEntries);
        
        int memberIdStart = 0;
        MemberDataUpdate[] updates1 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        memberIdStart += subArraySize;
        MemberDataUpdate[] updates2 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        memberIdStart += subArraySize;
        MemberDataUpdate[] updates3 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        
        // 1st batch of updates
        for(MemberDataUpdate u : updates1) 
        {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        int testIndex = 0;
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize);
        
        // 2nd batch of updates
        for(MemberDataUpdate u : updates2) 
        {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 2);
        
        // 3rd batch of updates
        for(MemberDataUpdate u : updates3) 
        {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 3);
        
        // Random-update the entire array
        MemberDataUpdate[] updates = MemberDataUpdate.generateUpdates(0, array1.length());

        for(MemberDataUpdate u : updates) 
        {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        for(MemberDataUpdate u : updates) 
        {
            assert array1.get(u.getMemberId()) == u.getData();
        }
        
        // Create the second array, which should load data from cache
        DynamicLongArrayImpl array2 = new DynamicLongArrayImpl(
                TEST_OUTPUT_DIR,
                subArrayShift,
                maxEntrySize,
                maxEntries);
        assertTrue("array2.getHWMark() is greater than array1.getHWMark()", array2.getHWMark() <= array1.getHWMark());
        
        // Persist the first array
        array1.persist();
        
        // Create the third array, which should load data from cache
        DynamicLongArrayImpl array3 = new DynamicLongArrayImpl(
                TEST_OUTPUT_DIR,
                subArrayShift,
                maxEntrySize,
                maxEntries);
        assertTrue("array3.getHWMark() is greater than array1.getHWMark()", array3.getHWMark() <= array1.getHWMark());
        
        int nonZeroCount = 0;
        for(int index = 0; index < array1.length(); index++)
        {
            if (array1.get(index) > 0) nonZeroCount++;
            assert array1.get(index) == array3.get(index);
        }
        
        assertTrue("all zeros in array1", nonZeroCount > 0);
        
        cleanTestOutput();
    }
    
    @Test
    public void testDynamicShortArray() throws Exception
    {
        cleanTestOutput();
        
        // Create the first long array and do random updates
        DynamicShortArrayImpl array1 = new DynamicShortArrayImpl(
                TEST_OUTPUT_DIR,
                subArrayShift,
                maxEntrySize,
                maxEntries);
        
        int memberIdStart = 0;
        MemberDataUpdate[] updates1 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        memberIdStart += subArraySize;
        MemberDataUpdate[] updates2 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        memberIdStart += subArraySize;
        MemberDataUpdate[] updates3 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        
        // 1st batch of updates
        for(MemberDataUpdate u : updates1) 
        {
            array1.set(u.getMemberId(), (short)u.getData(), u.getScn());
        }
        
        int testIndex = 0;
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize);
        
        // 2nd batch of updates
        for(MemberDataUpdate u : updates2) 
        {
            array1.set(u.getMemberId(), (short)u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 2);
        
        // 3rd batch of updates
        for(MemberDataUpdate u : updates3) 
        {
            array1.set(u.getMemberId(), (short)u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 3);
        
        // Random-update the entire array
        MemberDataUpdate[] updates = MemberDataUpdate.generateUpdates(0, array1.length());

        for(MemberDataUpdate u : updates) 
        {
            array1.set(u.getMemberId(), (short)u.getData(), u.getScn());
        }
        
        for(MemberDataUpdate u : updates) 
        {
            assert array1.get(u.getMemberId()) == (short)u.getData();
        }
        
        // Create the second array, which should load data from cache
        DynamicShortArrayImpl array2 = new DynamicShortArrayImpl(
                TEST_OUTPUT_DIR,
                subArrayShift,
                maxEntrySize,
                maxEntries);
        assertTrue("array2.getHWMark() is greater than array1.getHWMark()", array2.getHWMark() <= array1.getHWMark());
        
        // Persist the first array
        array1.persist();
        
        // Create the third array, which should load data from cache
        DynamicShortArrayImpl array3 = new DynamicShortArrayImpl(
                TEST_OUTPUT_DIR,
                subArrayShift,
                maxEntrySize,
                maxEntries);
        assertTrue("array3.getHWMark() is greater than array1.getHWMark()", array3.getHWMark() <= array1.getHWMark());
        
        int nonZeroCount = 0;
        for(int index = 0; index < array1.length(); index++)
        {
            if (array1.get(index) > 0) nonZeroCount++;
            assert array1.get(index) == array3.get(index);
        }
        
        assertTrue("all zeros in array1", nonZeroCount > 0);
        
        cleanTestOutput();
    }
}
