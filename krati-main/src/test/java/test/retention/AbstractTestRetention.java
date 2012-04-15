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

package test.retention;

import java.io.File;
import java.util.Random;

import test.retention.util.RandomClockFactory;
import test.retention.util.RetentionReaderThread;
import test.util.DirUtils;

import junit.framework.TestCase;
import krati.io.Serializer;
import krati.retention.Retention;
import krati.retention.Event;
import krati.retention.EventBatchSerializer;
import krati.retention.Position;
import krati.retention.SimpleEventBatchSerializer;
import krati.retention.clock.Clock;
import krati.retention.clock.ClockSerializer;
import krati.retention.clock.Occurred;
import krati.retention.policy.RetentionPolicy;
import krati.retention.policy.RetentionPolicyOnSize;

/**
 * AbstractTestRetention
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/09, 2011 - Created <br/>
 * 10/17, 2011 - Added testFlush <br/>
 */
public abstract class AbstractTestRetention<T> extends TestCase {
    protected Retention<T> _retention;
    protected Random _rand = new Random();
    protected RandomClockFactory _clockFactory = new RandomClockFactory(3);
    
    protected File getHomeDir() {
        return DirUtils.getTestDir(getClass());
    }

    protected int getId() {
        return 2;
    }
    
    protected int getEventBatchSize() {
        return 1000;
    }
    
    protected int getNumRetentionBatches() {
        return 20;
    }
    
    protected RetentionPolicy createRetentionPolicy() {
        return new RetentionPolicyOnSize(getNumRetentionBatches());
    }
    
    protected EventBatchSerializer<T> createBatchSerializer() {
        return new SimpleEventBatchSerializer<T>(createEventValueSerializer(), createEventClockSerializer());
    }
    
    protected Serializer<Clock> createEventClockSerializer() {
        return new ClockSerializer();
    }
    
    protected abstract Serializer<T> createEventValueSerializer();
    
    protected abstract Retention<T> createRetention() throws Exception;
    
    protected abstract Event<T> nextEvent(Clock clock);
    
    @Override
    protected void setUp() {
        try {
            DirUtils.deleteDirectory(getHomeDir());
            _retention = createRetention();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            _retention.close();
            DirUtils.deleteDirectory(getHomeDir());
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            _retention = null;
        }
    }
    
    public void testApi() throws Exception {
        int cnt = getEventBatchSize() + _rand.nextInt(getNumRetentionBatches() * getEventBatchSize() / 2);
        Clock clock = _clockFactory.next();
        Clock minClock = clock;
        _retention.put(nextEvent(clock));
        
        // Create an idle clock
        Clock idleClock0 = _clockFactory.next();
        
        clock = _clockFactory.next();
        _retention.put(nextEvent(clock));
        
        // Create an idle clock
        Clock idleClock1 = _clockFactory.next();
        
        for(int i = 2; i < cnt; i++) {
            clock = _clockFactory.next();
            _retention.put(nextEvent(clock));
        }
        Clock maxClock = clock;
        
        assertEquals(getId(), _retention.getId());
        
        Position pos = _retention.getPosition();
        assertEquals((long)cnt, pos.getOffset());
        assertEquals(cnt, _retention.getOffset());
        
        assertTrue(minClock.compareTo(_retention.getMinClock()) == Occurred.EQUICONCURRENTLY);
        assertTrue(maxClock.compareTo(_retention.getMaxClock()) == Occurred.EQUICONCURRENTLY);
        
        Position sincePosition = _retention.getPosition(idleClock0);
        assertEquals(getId(), sincePosition.getId());
        assertEquals(0, sincePosition.getOffset());
        
        sincePosition = _retention.getPosition(idleClock1);
        assertEquals(getId(), sincePosition.getId());
        assertEquals(1, sincePosition.getOffset());
        
        Retention<T> retention2 = createRetention();
        assertTrue(minClock.compareTo(retention2.getMinClock()) == Occurred.EQUICONCURRENTLY);
        assertTrue(retention2.getMaxClock().beforeEqual(_retention.getMaxClock()));
        
        sincePosition = retention2.getPosition(idleClock0);
        assertEquals(getId(), sincePosition.getId());
        assertEquals(0, sincePosition.getOffset());
        
        sincePosition = _retention.getPosition(idleClock1);
        assertEquals(getId(), sincePosition.getId());
        assertEquals(1, sincePosition.getOffset());
    }
    
    public void testFlush() throws Exception {
        int cnt = getEventBatchSize() * (1 + _rand.nextInt(10)) + 1000;
        
        // Start the Retention reader thread
        RetentionReaderThread<T> reader = new RetentionReaderThread<T>(_retention);
        reader.start();
        
        // Add new events into the Retention
        for(int i = 0; i < cnt; i++) {
            Clock clock = _clockFactory.next();
            _retention.put(nextEvent(clock));
            
            // Random flush
            if(_rand.nextFloat() < 0.05f) {
                _retention.flush();
            }
        }
        
        // Final flush
        _retention.flush();
        
        // Stop the Retention reader thread
        reader.stop(_retention.getOffset());
        reader.join();
        
        assertEquals(_retention.getOffset(), reader.getReadCount());
        
        Clock minClock = _retention.getMinClock();
        Clock maxClock = _retention.getMaxClock();
        assertTrue(minClock.after(Clock.ZERO));
        assertTrue(minClock.compareTo(maxClock) == Occurred.BEFORE);
        
        // Close retention
        _retention.close();
    }
}
