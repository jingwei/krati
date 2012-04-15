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

package test.core.api;

import org.junit.Test;

import test.AbstractTest;
import test.core.MemberDataUpdate;

import java.util.Random;

import krati.core.array.basic.DynamicConstants;
import krati.core.array.basic.DynamicIntArray;
import krati.core.array.basic.DynamicLongArray;
import krati.core.array.basic.DynamicShortArray;

/**
 * TestDynamicArray
 * 
 * @author jwu
 * 
 */
public class TestDynamicArray extends AbstractTest {
    static long scn = 0;
    static Random random = new Random(System.currentTimeMillis());
    
    int subArrayBits = DynamicConstants.SUB_ARRAY_BITS;
    int subArraySize = DynamicConstants.SUB_ARRAY_SIZE;
    int maxEntrySize = 1000;
    int maxEntries = 10;
    
    public TestDynamicArray() {
        super(TestDynamicArray.class.getCanonicalName());
    }
    
    @Test
    public void testDynamicIntArray() throws Exception {
        cleanTestOutput();
        
        // Create the first long array and do random updates
        DynamicIntArray array1 = new DynamicIntArray(maxEntrySize, maxEntries, TEST_OUTPUT_DIR);
        
        int memberIdStart = 0;
        MemberDataUpdate[] updates1 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        memberIdStart += subArraySize;
        MemberDataUpdate[] updates2 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        memberIdStart += subArraySize;
        MemberDataUpdate[] updates3 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        
        // 1st batch of updates
        for (MemberDataUpdate u : updates1) {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        int testIndex = 0;
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize);
        
        // 2nd batch of updates
        for (MemberDataUpdate u : updates2) {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 2);
        
        // 3rd batch of updates
        for (MemberDataUpdate u : updates3) {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 3);
        
        // Random-update the entire array
        MemberDataUpdate[] updates = MemberDataUpdate.generateUpdates(0, array1.length());

        for (MemberDataUpdate u : updates) {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        for (MemberDataUpdate u : updates) {
            assertEquals(array1.get(u.getMemberId()), u.getData());
        }
        
        // Sync the first array
        array1.sync();
        
        // Create the second array, which should load data from cache
        DynamicIntArray array2 = new DynamicIntArray(maxEntrySize, maxEntries, TEST_OUTPUT_DIR);
        assertTrue("array2.getHWMark() is greater than array1.getHWMark()", array2.getHWMark() <= array1.getHWMark());
        
        int nonZeroCount = 0;
        for (int index = 0; index < array1.length(); index++) {
            if (array1.get(index) > 0) nonZeroCount++;
            assertEquals(array1.get(index), array2.get(index));
        }
        
        assertTrue("all zeros in array1", nonZeroCount > 0);
        
        cleanTestOutput();
    }
    
    @Test
    public void testDynamicLongArray() throws Exception {
        cleanTestOutput();
        
        // Create the first long array and do random updates
        DynamicLongArray array1 = new DynamicLongArray(maxEntrySize, maxEntries, TEST_OUTPUT_DIR);
        
        int memberIdStart = 0;
        MemberDataUpdate[] updates1 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        memberIdStart += subArraySize;
        MemberDataUpdate[] updates2 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        memberIdStart += subArraySize;
        MemberDataUpdate[] updates3 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        
        // 1st batch of updates
        for (MemberDataUpdate u : updates1) {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        int testIndex = 0;
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize);
        
        // 2nd batch of updates
        for (MemberDataUpdate u : updates2) {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 2);
        
        // 3rd batch of updates
        for (MemberDataUpdate u : updates3) {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 3);
        
        // Random-update the entire array
        MemberDataUpdate[] updates = MemberDataUpdate.generateUpdates(0, array1.length());

        for (MemberDataUpdate u : updates) {
            array1.set(u.getMemberId(), u.getData(), u.getScn());
        }
        
        for (MemberDataUpdate u : updates) {
            assertEquals(array1.get(u.getMemberId()), u.getData());
        }
        
        // Sync the first array
        array1.sync();
        
        // Create the second array, which should load data from cache
        DynamicLongArray array2 = new DynamicLongArray(maxEntrySize, maxEntries, TEST_OUTPUT_DIR);
        assertTrue("array3.getHWMark() is greater than array1.getHWMark()", array2.getHWMark() <= array1.getHWMark());
        
        int nonZeroCount = 0;
        for (int index = 0; index < array1.length(); index++) {
            if (array1.get(index) > 0) nonZeroCount++;
            assertEquals(array1.get(index), array2.get(index));
        }
        
        assertTrue("all zeros in array1", nonZeroCount > 0);
        
        cleanTestOutput();
    }
    
    @Test
    public void testDynamicShortArray() throws Exception {
        cleanTestOutput();
        
        // Create the first long array and do random updates
        DynamicShortArray array1 = new DynamicShortArray(maxEntrySize, maxEntries, TEST_OUTPUT_DIR);
        
        int memberIdStart = 0;
        MemberDataUpdate[] updates1 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        memberIdStart += subArraySize;
        MemberDataUpdate[] updates2 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        memberIdStart += subArraySize;
        MemberDataUpdate[] updates3 = MemberDataUpdate.generateUpdates(memberIdStart, subArraySize);
        
        // 1st batch of updates
        for (MemberDataUpdate u : updates1) {
            array1.set(u.getMemberId(), (short)u.getData(), u.getScn());
        }
        
        int testIndex = 0;
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize);
        
        // 2nd batch of updates
        for (MemberDataUpdate u : updates2) {
            array1.set(u.getMemberId(), (short)u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 2);
        
        // 3rd batch of updates
        for (MemberDataUpdate u : updates3) {
            array1.set(u.getMemberId(), (short)u.getData(), u.getScn());
        }
        
        assertTrue("member " + testIndex + " is not in range", array1.hasIndex(testIndex));
        testIndex += subArraySize;
        assertTrue("member " + testIndex + " is in range", !array1.hasIndex(testIndex));
        assertTrue("incorrect array size", array1.length() == subArraySize * 3);
        
        // Random-update the entire array
        MemberDataUpdate[] updates = MemberDataUpdate.generateUpdates(0, array1.length());

        for (MemberDataUpdate u : updates) {
            array1.set(u.getMemberId(), (short)u.getData(), u.getScn());
        }
        
        for (MemberDataUpdate u : updates) {
            assertEquals(array1.get(u.getMemberId()), (short)u.getData());
        }
        
        // Sync the first array
        array1.sync();
        
        // Create the second array, which should load data from cache
        DynamicShortArray array2 = new DynamicShortArray(maxEntrySize, maxEntries, TEST_OUTPUT_DIR);
        assertTrue("array2.getHWMark() is greater than array1.getHWMark()", array2.getHWMark() <= array1.getHWMark());
        
        int nonZeroCount = 0;
        for (int index = 0; index < array1.length(); index++) {
            if (array1.get(index) > 0) nonZeroCount++;
            assertEquals(array1.get(index), array2.get(index));
        }
        
        assertTrue("all zeros in array1", nonZeroCount > 0);
        
        cleanTestOutput();
    }
}
