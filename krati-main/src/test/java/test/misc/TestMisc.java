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

package test.misc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import test.util.DirUtils;

import junit.framework.TestCase;
import krati.core.array.entry.Entry;
import krati.core.array.entry.EntryUtility;
import krati.core.array.entry.EntryValueLong;
import krati.core.array.entry.PreFillEntryLong;

/**
 * TestMisc
 * 
 * @author jwu
 * @since 04/20, 2012
 */
public class TestMisc extends TestCase {
    Random _rand = new Random();
    
    public void testArrayListRemoveLast() {
        List<Object> list = new ArrayList<Object>();
        list.add(new Object());
        
        int cnt = _rand.nextInt(100);
        for(int i = 0; i < cnt; i++) {
            list.add(new Object());
        }
        
        int size = list.size();
        while(size > 0) {
            list.set(size-1, null);
            assertEquals(size, list.size());
            assertTrue(list.get(size-1) == null);
            list.remove(size - 1);
            assertEquals(size - 1, list.size());
            size = list.size();
        }
    }
    
    public void testGetEntryId() {
        int cnt = _rand.nextInt(1000) + 1;
        for(int i = 0; i < cnt; i++) {
            long entryId1 = _rand.nextInt(Integer.MAX_VALUE);
            String fileName = getEntryFileName(entryId1);
            long entryId2 = EntryUtility.getEntryId(fileName);
            assertEquals(entryId1, entryId2);
        }
    }
    
    public void testSortEntriesById() throws IOException {
        int cnt = _rand.nextInt(1000) + 1;
        long startId = System.currentTimeMillis();
        File testDir = DirUtils.getTestDir(getClass());
        List<Entry<EntryValueLong>> entryList = new ArrayList<Entry<EntryValueLong>>();
        
        for(int i = cnt - 1; i >= 0; i--) {
            String fileName = getEntryFileName(startId + i);
            File file = new File(testDir, fileName);
            PreFillEntryLong entry = new PreFillEntryLong(100);
            entry.save(file);
            entryList.add(entry);
        }
        
        EntryUtility.sortEntriesById(entryList);
        
        for(int i = 0; i < cnt; i++) {
            String fileName = entryList.get(i).getFile().getName();
            long entryId = EntryUtility.getEntryId(fileName);
            assertEquals(startId + i, entryId);
        }
        
        DirUtils.deleteDirectory(testDir);
    }
    
    protected String getEntryFileName(long entryId) {
        long minScn = System.currentTimeMillis();
        long maxScn = minScn + 100000;
        String fileName = "entry" + "_" + entryId + "_" + minScn + "_" + maxScn + ".idx";
        return fileName;
    }
}
