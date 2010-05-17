package test.cds;

import java.util.Random;

import test.AbstractTest;

import krati.cds.array.IntArray;
import krati.cds.array.LongArray;
import krati.cds.array.ShortArray;
import krati.cds.impl.array.DynamicIntArrayImpl;
import krati.cds.impl.array.DynamicLongArrayImpl;
import krati.cds.impl.array.DynamicShortArrayImpl;

/**
 * TestArray
 * 
 * @author jwu
 *
 */
public class TestArray extends AbstractTest
{
    static long scn = 0;
    static Random random = new Random(System.currentTimeMillis());
    
    public TestArray()
    {
        super(TestArray.class.getCanonicalName());
    }
    
    public void testIntArray() throws Exception
    {
        cleanTestOutput();
        
        // Create the first long array and do random updates
        int subArrayShift = 16;
        int subArraySize = 1 << subArrayShift;
        int maxEntrySize = 500;
        int maxEntries = 10;
        
        IntArray array1 = new DynamicIntArrayImpl(TEST_OUTPUT_DIR,
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
            array1.setData(u.getMemberId(), u.getData(), u.getScn());
        }
        
        int testIndex = 0;
        assertTrue("member " + testIndex + " is not in range", array1.indexInRange(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.indexInRange(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize);
        
        // 2nd batch of updates
        for(MemberDataUpdate u : updates2) 
        {
            array1.setData(u.getMemberId(), u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.indexInRange(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.indexInRange(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 2);
        
        // 3rd batch of updates
        for(MemberDataUpdate u : updates3) 
        {
            array1.setData(u.getMemberId(), u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.indexInRange(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.indexInRange(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 3);
        
        // Random-update the entire array
        MemberDataUpdate[] updates = MemberDataUpdate.generateUpdates(0, array1.length());

        for(MemberDataUpdate u : updates) 
        {
            array1.setData(u.getMemberId(), u.getData(), u.getScn());
        }
        
        for(MemberDataUpdate u : updates) 
        {
            assert array1.getData(u.getMemberId()) == u.getData();
        }
        
        // Create the second array, which should load data from cache
        IntArray array2 = new DynamicIntArrayImpl(TEST_OUTPUT_DIR,
                                                  subArrayShift,
                                                  maxEntrySize,
                                                  maxEntries);
        assertTrue("array2.getHWMark() is greater than array1.getHWMark()", array2.getHWMark() <= array1.getHWMark());
        
        // Persist the first array
        array1.persist();
        
        // Create the third array, which should load data from cache
        IntArray array3 = new DynamicIntArrayImpl(TEST_OUTPUT_DIR,
                                                  subArrayShift,
                                                  maxEntrySize,
                                                  maxEntries);
        assertTrue("array3.getHWMark() is greater than array1.getHWMark()", array3.getHWMark() <= array1.getHWMark());
        
        int nonZeroCount = 0;
        for(int index = 0; index < array1.length(); index++)
        {
            if (array1.getData(index) > 0) nonZeroCount++;
            assert array1.getData(index) == array3.getData(index);
        }
        
        assertTrue("all zeros in array1", nonZeroCount > 0);
        
        cleanTestOutput();
    }
    
    public void testLongArray() throws Exception
    {
        cleanTestOutput();
        
        // Create the first long array and do random updates
        int subArrayShift = 16;
        int subArraySize = 1 << subArrayShift;
        int maxEntrySize = 500;
        int maxEntries = 10;
        
        LongArray array1 = new DynamicLongArrayImpl(TEST_OUTPUT_DIR,
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
            array1.setData(u.getMemberId(), u.getData(), u.getScn());
        }
        
        int testIndex = 0;
        assertTrue("member " + testIndex + " is not in range", array1.indexInRange(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.indexInRange(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize);
        
        // 2nd batch of updates
        for(MemberDataUpdate u : updates2) 
        {
            array1.setData(u.getMemberId(), u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.indexInRange(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.indexInRange(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 2);
        
        // 3rd batch of updates
        for(MemberDataUpdate u : updates3) 
        {
            array1.setData(u.getMemberId(), u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.indexInRange(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.indexInRange(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 3);
        
        // Random-update the entire array
        MemberDataUpdate[] updates = MemberDataUpdate.generateUpdates(0, array1.length());

        for(MemberDataUpdate u : updates) 
        {
            array1.setData(u.getMemberId(), u.getData(), u.getScn());
        }
        
        for(MemberDataUpdate u : updates) 
        {
            assert array1.getData(u.getMemberId()) == u.getData();
        }
        
        // Create the second array, which should load data from cache
        LongArray array2 = new DynamicLongArrayImpl(TEST_OUTPUT_DIR,
                                                    subArrayShift,
                                                    maxEntrySize,
                                                    maxEntries);
        assertTrue("array2.getHWMark() is greater than array1.getHWMark()", array2.getHWMark() <= array1.getHWMark());
        
        // Persist the first array
        array1.persist();
        
        // Create the third array, which should load data from cache
        LongArray array3 = new DynamicLongArrayImpl(TEST_OUTPUT_DIR,
                                                    subArrayShift,
                                                    maxEntrySize,
                                                    maxEntries);
        assertTrue("array3.getHWMark() is greater than array1.getHWMark()", array3.getHWMark() <= array1.getHWMark());
        
        int nonZeroCount = 0;
        for(int index = 0; index < array1.length(); index++)
        {
            if (array1.getData(index) > 0) nonZeroCount++;
            assert array1.getData(index) == array3.getData(index);
        }
        
        assertTrue("all zeros in array1", nonZeroCount > 0);
        
        cleanTestOutput();
    }


    public void testShortArray() throws Exception
    {
        cleanTestOutput();
        
        // Create the first long array and do random updates
        int subArrayShift = 16;
        int subArraySize = 1 << subArrayShift;
        int maxEntrySize = 500;
        int maxEntries = 10;
        
        ShortArray array1 = new DynamicShortArrayImpl(TEST_OUTPUT_DIR,
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
            array1.setData(u.getMemberId(), (short)u.getData(), u.getScn());
        }
        
        int testIndex = 0;
        assertTrue("member " + testIndex + " is not in range", array1.indexInRange(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.indexInRange(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize);
        
        // 2nd batch of updates
        for(MemberDataUpdate u : updates2) 
        {
            array1.setData(u.getMemberId(), (short)u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.indexInRange(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.indexInRange(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 2);
        
        // 3rd batch of updates
        for(MemberDataUpdate u : updates3) 
        {
            array1.setData(u.getMemberId(), (short)u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.indexInRange(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.indexInRange(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 3);
        
        // Random-update the entire array
        MemberDataUpdate[] updates = MemberDataUpdate.generateUpdates(0, array1.length());

        for(MemberDataUpdate u : updates) 
        {
            array1.setData(u.getMemberId(), (short)u.getData(), u.getScn());
        }
        
        for(MemberDataUpdate u : updates) 
        {
            assert array1.getData(u.getMemberId()) == (short)u.getData();
        }
        
        // Create the second array, which should load data from cache
        ShortArray array2 = new DynamicShortArrayImpl(TEST_OUTPUT_DIR,
                                                      subArrayShift,
                                                      maxEntrySize,
                                                      maxEntries);
        assertTrue("array2.getHWMark() is greater than array1.getHWMark()", array2.getHWMark() <= array1.getHWMark());
        
        // Persist the first array
        array1.persist();
        
        // Create the third array, which should load data from cache
        ShortArray array3 = new DynamicShortArrayImpl(TEST_OUTPUT_DIR,
                                                      subArrayShift,
                                                      maxEntrySize,
                                                      maxEntries);
        assertTrue("array3.getHWMark() is greater than array1.getHWMark()", array3.getHWMark() <= array1.getHWMark());
        
        int nonZeroCount = 0;
        for(int index = 0; index < array1.length(); index++)
        {
            if (array1.getData(index) > 0) nonZeroCount++;
            assert array1.getData(index) == array3.getData(index);
        }
        
        assertTrue("all zeros in array1", nonZeroCount > 0);
        
        cleanTestOutput();
    }
}
