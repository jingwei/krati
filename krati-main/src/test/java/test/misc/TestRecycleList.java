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

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

/**
 * TestRecycleList
 * 
 * @author jwu
 * @since 02/14, 2012
 */
public class TestRecycleList extends TestCase {
    private int _recycleLimit = 1; // Greater than 0
    private LinkedList<Integer> _recycleList = new LinkedList<Integer>();
    
    private boolean recycle(int seg) {
        if(_recycleList.isEmpty()) {
            _recycleList.add(seg);
            return true;
        }
        
        int index = 0;
        int count = _recycleList.size();
        Iterator<Integer> iter = _recycleList.iterator();
        
        while(iter.hasNext()) {
            int val = iter.next();
            if(val == seg) {
                return false; // NOT FEASIBLE
            } else if(val > seg) {
                break;
            }
            index++;
        }
        
        if(count < _recycleLimit) {
            _recycleList.add(index, seg);
            return true;
        } else if(_recycleList.get(count - 1) > seg) {
            _recycleList.add(index, seg);
            _recycleList.removeLast();
            return true;
        } else {
            return false;
        }
    }
    
    public void testRecycleList1() {
        _recycleLimit = 1;
        _recycleList.clear();
        
        assertTrue(recycle(9));
        assertEquals(9, _recycleList.get(0).intValue());
        assertEquals(1, _recycleList.size());
        
        assertFalse(recycle(10));
        assertEquals(9, _recycleList.get(0).intValue());
        assertEquals(1, _recycleList.size());
        
        assertTrue(recycle(4));
        assertEquals(4, _recycleList.get(0).intValue());
        assertEquals(1, _recycleList.size());
        
        assertTrue(recycle(0));
        assertEquals(0, _recycleList.get(0).intValue());
        assertEquals(1, _recycleList.size());
        
        assertFalse(recycle(9));
        assertEquals(0, _recycleList.get(0).intValue());
        assertEquals(1, _recycleList.size());
    }
    
    public void testRecycleList2() {
        _recycleLimit = 2;
        _recycleList.clear();
        
        // Recycle 9, 7
        assertTrue(recycle(9));
        assertEquals(9, _recycleList.get(0).intValue());
        assertEquals(1, _recycleList.size());
        
        assertTrue(recycle(7));
        assertEquals(7, _recycleList.get(0).intValue());
        assertEquals(9, _recycleList.get(1).intValue());
        assertEquals(2, _recycleList.size());
        
        // Recycle 7, 9
        _recycleList.clear();
        assertTrue(recycle(7));
        assertEquals(7, _recycleList.get(0).intValue());
        assertEquals(1, _recycleList.size());
        
        assertTrue(recycle(9));
        assertEquals(7, _recycleList.get(0).intValue());
        assertEquals(9, _recycleList.get(1).intValue());
        assertEquals(2, _recycleList.size());
        
        // Recycle 4
        assertTrue(recycle(4));
        assertEquals(4, _recycleList.get(0).intValue());
        assertEquals(7, _recycleList.get(1).intValue());
        assertEquals(2, _recycleList.size());
        
        // Recycle 6
        assertTrue(recycle(6));
        assertEquals(4, _recycleList.get(0).intValue());
        assertEquals(6, _recycleList.get(1).intValue());
        assertEquals(2, _recycleList.size());
        
        // Recycle 6
        assertFalse(recycle(6));
        assertEquals(4, _recycleList.get(0).intValue());
        assertEquals(6, _recycleList.get(1).intValue());
        assertEquals(2, _recycleList.size());
        
        // Recycle 9
        assertFalse(recycle(9));
        assertEquals(4, _recycleList.get(0).intValue());
        assertEquals(6, _recycleList.get(1).intValue());
        assertEquals(2, _recycleList.size());
    }
    
    public void testRecycleListN() {
        Random rand = new Random();
        _recycleLimit = 1 + rand.nextInt(10);
        _recycleList.clear();
        
        int cnt = 1000 + rand.nextInt(1000);
        
        assertTrue(recycle(10000));
        while(cnt-- > 0) {
            int seg = rand.nextInt(10000);
            Set<Integer> set = new HashSet<Integer>();
            
            // Verify that Segment Id(s) are ordered in natural order
            Iterator<Integer> iter = _recycleList.iterator();
            if(iter.hasNext()) {
                int prev = iter.next();
                set.add(prev);
                while (iter.hasNext()) {
                    int next = iter.next();
                    assertTrue(prev < next);
                    prev = next;
                    set.add(prev);
                }
            }
            
            if(set.add(seg) && (_recycleList.size() < _recycleLimit || _recycleList.get(_recycleList.size() - 1) > seg)) {
                assertTrue(recycle(seg));
            } else {
                assertFalse(recycle(seg));
            }
            
            assertTrue(_recycleList.size() <= _recycleLimit);
            
            if(_recycleList.size() > 0 && rand.nextFloat() < 0.2f) {
                seg = _recycleList.removeFirst();
            }
        }
    }
}
