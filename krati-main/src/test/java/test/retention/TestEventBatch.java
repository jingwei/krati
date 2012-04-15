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

import java.util.ArrayList;
import java.util.Random;

import test.retention.util.RandomClockFactory;


import junit.framework.TestCase;
import krati.io.serializer.StringSerializer;
import krati.retention.Event;
import krati.retention.EventBatch;
import krati.retention.EventBatchHeader;
import krati.retention.SimpleEvent;
import krati.retention.SimpleEventBatch;
import krati.retention.SimpleEventBatchSerializer;
import krati.retention.clock.Clock;
import krati.retention.clock.ClockSerializer;
import krati.retention.clock.Occurred;

/**
 * TestEventBatch
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/14, 2011 - Created
 */
public class TestEventBatch extends TestCase {
    protected Random _rand = new Random();
    protected RandomClockFactory _randClockFactory = new RandomClockFactory(2);
    
    public void testApiBasics() {
        EventBatch<String> batch;
        long origin = _rand.nextInt(Integer.MAX_VALUE);
        Clock clock = _randClockFactory.next();
        int capacity = Math.max(EventBatch.MINIMUM_BATCH_SIZE, _rand.nextInt(2000));
        
        batch = new SimpleEventBatch<String>(origin, clock, capacity);
        
        assertEquals(EventBatch.VERSION, batch.getVersion());
        assertEquals(0, batch.getSize());
        assertEquals(origin, batch.getOrigin());
        assertEquals(Occurred.EQUICONCURRENTLY, clock.compareTo(batch.getMinClock()));
        assertEquals(Occurred.EQUICONCURRENTLY, clock.compareTo(batch.getMaxClock()));
        
        // Add the first event
        clock = _randClockFactory.next();
        batch.put(new SimpleEvent<String>("Event." + clock, clock));
        
        assertTrue(clock.compareTo(batch.getMinClock()) == Occurred.EQUICONCURRENTLY);
        assertTrue(clock.compareTo(batch.getMaxClock()) == Occurred.EQUICONCURRENTLY);
        assertEquals(1, batch.getSize());
        
        // Add the second event
        clock = _randClockFactory.next();
        batch.put(new SimpleEvent<String>("Event." + clock, clock));
        
        assertTrue(clock.after(batch.getMinClock()));
        assertTrue(clock.compareTo(batch.getMaxClock()) == Occurred.EQUICONCURRENTLY);
        assertEquals(2, batch.getSize());
        
        do {
            clock = _randClockFactory.next();
        } while(batch.put(new SimpleEvent<String>("Event." + clock, clock)));
        
        assertTrue(clock.after(batch.getMinClock()));
        assertTrue(clock.after(batch.getMaxClock()));
        assertEquals(capacity, batch.getSize());
        
        batch.setCompletionTime(System.currentTimeMillis() + 1);
        assertTrue(batch.getCreationTime() < batch.getCompletionTime());
        
        EventBatchHeader header = batch.getHeader();
        assertEquals(batch.getVersion(), header.getVersion());
        assertEquals(batch.getSize(), header.getSize());
        assertEquals(batch.getOrigin(), header.getOrigin());
        assertEquals(batch.getCreationTime(), header.getCreationTime());
        assertEquals(batch.getCompletionTime(), header.getCompletionTime());
        assertTrue(header.getMinClock().compareTo(batch.getMinClock()) == Occurred.EQUICONCURRENTLY);
        assertTrue(header.getMaxClock().compareTo(batch.getMaxClock()) == Occurred.EQUICONCURRENTLY);
        
        long nextOffset;
        ArrayList<Event<String>> list = new ArrayList<Event<String>>();
        nextOffset = batch.get(batch.getOrigin(), list);
        assertEquals(batch.getSize(), list.size());
        assertEquals(batch.getOrigin() + batch.getSize(), nextOffset);
        
        ArrayList<Event<String>> list2 = new ArrayList<Event<String>>();
        int num = _rand.nextInt(batch.getSize());
        nextOffset = batch.get(batch.getOrigin() + num, list2);
        assertEquals(batch.getSize() - num, list2.size());
        assertEquals(batch.getOrigin() + batch.getSize(), nextOffset);
        
        ArrayList<Event<String>> list3 = new ArrayList<Event<String>>();
        nextOffset = batch.get(batch.getOrigin() + batch.getSize(), list3);
        assertEquals(0, list3.size());
        assertEquals(batch.getOrigin() + batch.getSize(), nextOffset);
    }
    
    public void testEventBatchSerializer() {
        long origin = _rand.nextInt(Integer.MAX_VALUE);
        Clock clock = _randClockFactory.next();
        
        EventBatch<String> batch = new SimpleEventBatch<String>(origin, clock);
        do {
            clock = _randClockFactory.next();
        } while(batch.put(new SimpleEvent<String>("Event." + clock, clock)));
        batch.setCompletionTime(System.currentTimeMillis());
        
        EventBatchHeader header = batch.getHeader();
        
        SimpleEventBatchSerializer<String> batchSerializer =
            new SimpleEventBatchSerializer<String>(new StringSerializer(), new ClockSerializer());
        byte[] bytes = batchSerializer.serialize(batch);
        EventBatch<String> batch2 = batchSerializer.deserialize(bytes);
        EventBatchHeader header2 = batch2.getHeader();
        
        assertEquals(header.getVersion(), header2.getVersion());
        assertEquals(header.getSize(), header2.getSize());
        assertEquals(header.getOrigin(), header2.getOrigin());
        assertEquals(header.getCreationTime(), header2.getCreationTime());
        assertEquals(header.getCompletionTime(), header2.getCompletionTime());
        assertTrue(header.getMinClock().compareTo(header2.getMinClock()) == Occurred.EQUICONCURRENTLY);
        assertTrue(header.getMaxClock().compareTo(header2.getMaxClock()) == Occurred.EQUICONCURRENTLY);
    }
}
