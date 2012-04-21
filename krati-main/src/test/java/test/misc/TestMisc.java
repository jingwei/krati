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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

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
}
