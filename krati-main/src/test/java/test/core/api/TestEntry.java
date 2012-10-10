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

import java.io.File;
import java.util.Random;

import test.AbstractTest;

import krati.core.array.entry.Entry;
import krati.core.array.entry.EntryUtility;
import krati.core.array.entry.EntryValueInt;
import krati.core.array.entry.EntryValueIntFactory;
import krati.core.array.entry.SimpleEntry;

/**
 * Test Entry
 * 
 * @author jwu
 *
 */
public class TestEntry extends AbstractTest {
    static Random random = new Random(System.currentTimeMillis());
    
    public TestEntry() {
        super(TestEntry.class.getCanonicalName());
    }
    
    /**
     * Write an entry to disk and then load it into a new entry.
     * Verify that these two entries are the same.
     */
    public void testWriteReadEntry() throws Exception {
        cleanTestOutput();
        
        for(int run = 1; run <= 10; run++) {
            File file = new File(TEST_OUTPUT_DIR, "entry_test" + run + ".dat");
            Entry<EntryValueInt> entry = new SimpleEntry<EntryValueInt>(new EntryValueIntFactory(), 1000);
            
            for(int i = 0; i < 1000; i++) {
                entry.add(new EntryValueInt(i, random.nextInt(50000), i));
            }
            entry.save(file);
            
            Entry<EntryValueInt> entryRead = new SimpleEntry<EntryValueInt>(new EntryValueIntFactory(), 1000);
            entryRead.load(file);
            
            assertTrue("Entry minSCNs don't match at run " + run,
                                 entry.getMinScn() == entryRead.getMinScn());
            assertTrue("Entry maxSCNs don't match at run " + run,
                                 entry.getMaxScn() == entryRead.getMaxScn());
            assertTrue("Arrays don't match at run " + run,
                                 entry.getValueList().equals(entryRead.getValueList()));
        }
        
        cleanTestOutput();
    }
    
    /**
     * Test SortEntriesByPos by having it sort values then 
     * iterate over them to insure they're sorted.
     */
    public void testSortedEntryValues() {
        int c;
        EntryValueIntFactory valFactory = new EntryValueIntFactory();
        
        c = 0;
        Entry<EntryValueInt> e1 = new SimpleEntry<EntryValueInt>(valFactory, 10);
        for (int i = 9; i >= 0; i--) {
            e1.add(new EntryValueInt(i, c, c++));
        }
        
        c = 0;
        Entry<EntryValueInt> e2 = new SimpleEntry<EntryValueInt>(valFactory, 10);
        for (int i = 19; i >= 10; i--) {
            e2.add(new EntryValueInt(i, c, c++));
        }
        
        @SuppressWarnings("unchecked")
        Entry<EntryValueInt>[] entries = new Entry[] {e1, e2};
        EntryValueInt[] values = EntryUtility.sortEntriesToValues(entries);
        
        // verify the values are in sorted order
        long pos = 0;
        for (EntryValueInt value : values) {
            assertEquals(value.pos, pos);
            pos++;
        }
    }
}
