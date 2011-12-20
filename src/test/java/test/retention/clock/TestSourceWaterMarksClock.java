/*
 * Copyright (c) 2010-2011 LinkedIn, Inc
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

package test.retention.clock;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import test.util.DirUtils;

import junit.framework.TestCase;
import krati.retention.clock.Clock;
import krati.retention.clock.Occurred;
import krati.retention.clock.SourceWaterMarksClock;
import krati.util.SourceWaterMarks;

/**
 * TestSourceWaterMarksClock
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/15, 2011 - Created <br/>
 * 10/07, 2011 - Added testFlush <br/>
 */
public class TestSourceWaterMarksClock extends TestCase {
    Random _rand = new Random();
    File _sourceWaterMarksFile = null;
    SourceWaterMarks _sourceWaterMarks = null;
    SourceWaterMarksClock _clock = null;
    
    @Override
    protected void setUp() {
        try {
            _sourceWaterMarksFile = new File(DirUtils.getTestDir(getClass()), "sourceWaterMarks.scn");
            _sourceWaterMarks = new SourceWaterMarks(_sourceWaterMarksFile);
            ArrayList<String> sources = new ArrayList<String>();
            sources.add("member_picture");
            sources.add("member_profile");
            sources.add("member_flex_store");
            sources.add("member_contact");
            _clock = new SourceWaterMarksClock(sources, _sourceWaterMarks);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    protected void tearDown() {
        try {
            DirUtils.deleteDirectory(DirUtils.getTestDir(getClass()));
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            _sourceWaterMarksFile = null;
            _sourceWaterMarks = null;
            _clock = null;
        }
    }
    
    public void testApiBasics() {
        Iterator<String> iter;
        Clock clock;
        
        iter = _clock.sourceIterator();
        while(iter.hasNext()) {
            String source = iter.next();
            assertEquals(0, _clock.getLWMScn(source));
            assertEquals(0, _clock.getHWMScn(source));
        }
        
        clock = _clock.current();
        
        // Save hwmScn
        iter = _clock.sourceIterator();
        while(iter.hasNext()) {
            String source = iter.next();
            long hwm = _clock.getHWMScn(source) + _rand.nextInt(100) + 1;
            _clock.updateHWMark(source, hwm);
            assertTrue(_clock.getLWMScn(source) < _clock.getHWMScn(source));
            assertTrue(clock.before(_clock.current()));
            clock = _clock.current();
        }
        
        assertTrue(clock.compareTo(_clock.current()) == Occurred.EQUICONCURRENTLY);
        
        // Sync water marks
        _clock.syncWaterMarks();
        
        // Check lwmScn = hwmScn
        iter = _clock.sourceIterator();
        while(iter.hasNext()) {
            String source = iter.next();
            assertEquals(_clock.getLWMScn(source), _clock.getHWMScn(source));
        }
        
        // Open a new source water marks clock
        List<String> sources2 = new ArrayList<String>();
        iter = _clock.sourceIterator();
        while(iter.hasNext()) {
            sources2.add(iter.next());
        }
        
        SourceWaterMarksClock clock2 = new SourceWaterMarksClock(sources2, _sourceWaterMarks);
        assertTrue(_clock.current().compareTo(clock2.current()) == Occurred.EQUICONCURRENTLY);
    }
    
    public void testFlush() {
        Iterator<String> iter;
        
        iter = _clock.sourceIterator();
        while(iter.hasNext()) {
            String source = iter.next();
            _clock.setHWMark(source, _clock.getHWMScn(source) + _rand.nextInt(1000) + 1);
            assertTrue(_clock.getLWMScn(source) < _clock.getHWMScn(source));
            assertEquals(0, _clock.getWaterMark(source, Clock.ZERO));
        }
        
        Clock current = _clock.current();
        
        iter = _clock.sourceIterator();
        while(iter.hasNext()) {
            String source = iter.next();
            assertEquals(_clock.getHWMScn(source), _clock.getWaterMark(source, current));
        }
        
        _clock.flush();
        
        // Open a new source water marks clock
        List<String> sources2 = new ArrayList<String>();
        iter = _clock.sourceIterator();
        while(iter.hasNext()) {
            sources2.add(iter.next());
        }
        
        SourceWaterMarksClock clock2 = new SourceWaterMarksClock(sources2, _sourceWaterMarks);
        assertTrue(_clock.current().compareTo(clock2.current()) == Occurred.EQUICONCURRENTLY);
    }
}
