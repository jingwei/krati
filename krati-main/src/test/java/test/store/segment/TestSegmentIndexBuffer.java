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

package test.store.segment;

import java.util.Random;

import junit.framework.TestCase;
import krati.core.segment.SegmentIndexBuffer;

/**
 * TestSegmentIndexBuffer
 * 
 * @author jwu
 * @since 08/25, 2012
 */
public class TestSegmentIndexBuffer extends TestCase {
    protected Random _rand = new Random();
    protected SegmentIndexBuffer _sib;
    
    protected SegmentIndexBuffer create() {
        return new SegmentIndexBuffer();
    }
    
    @Override
    protected void setUp() {
        _sib = create();
    }
    
    public void testApiBasics() {
        int index = 0;
        int offset = 0;
        
        // empty
        assertEquals(0, _sib.size());
        assertTrue(_sib.capacity() > 0);
        
        // add first
        _sib.add(index++, offset++);
        assertEquals(index, _sib.size());
        
        // add more
        int cnt = _sib.capacity() + _rand.nextInt(1000);
        for(int i = 0; i < cnt; i++) {
            _sib.add(index++, offset++);
            assertEquals(index, _sib.size());
        }
        assertTrue(cnt < _sib.capacity());
        
        // add more
        cnt = _sib.capacity() + _rand.nextInt(1000);
        SegmentIndexBuffer.IndexOffset reuse = new SegmentIndexBuffer.IndexOffset();
        for(int i = 0; i < cnt; i++) {
            reuse.reinit(index++, offset++);
            _sib.add(reuse);
            assertEquals(index, _sib.size());
        }
        
        // get
        int start, end;
        end = _sib.size();
        start = _rand.nextInt(_sib.size());
        for(int pos = start; pos < end; pos++) {
            SegmentIndexBuffer.IndexOffset elem = _sib.get(pos);
            assertEquals(pos, elem.getIndex());
        }
        
        start = _rand.nextInt(_sib.size());
        for(int pos = start; pos < end; pos++) {
            _sib.get(pos, reuse);
            assertEquals(pos, reuse.getIndex());
        }
        
        // clear
        _sib.clear();
        assertEquals(0, _sib.size());
        assertTrue(_sib.capacity() > 0);
    }
}
