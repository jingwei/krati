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
import java.util.List;

import krati.io.Serializer;
import krati.io.serializer.StringSerializer;
import krati.retention.Retention;
import krati.retention.RetentionConfig;
import krati.retention.Event;
import krati.retention.Position;
import krati.retention.SimpleEventBus;
import krati.retention.SimpleEvent;
import krati.retention.clock.Clock;
import krati.store.factory.DynamicObjectStoreFactory;
import krati.util.Chronos;

/**
 * TestSimpleEventBus
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/14, 2011 - Created
 */
public class TestSimpleEventBus extends AbstractTestRetention<String> {

    @Override
    protected Serializer<String> createEventValueSerializer() {
        return new StringSerializer();
    }
    
    @Override
    protected Event<String> nextEvent(Clock clock) {
        return new SimpleEvent<String>("Event." + clock, clock);
    }
    
    @Override
    protected Retention<String> createRetention() throws Exception {
        RetentionConfig<String> config = new RetentionConfig<String>(getId(), getHomeDir());
        config.setBatchSize(getEventBatchSize());
        config.setRetentionPolicy(createRetentionPolicy());
        config.setEventValueSerializer(createEventValueSerializer());
        config.setEventClockSerializer(createEventClockSerializer());
        config.setSnapshotClockStoreFactory(new DynamicObjectStoreFactory<String, Clock>());
        return new SimpleEventBus<String>(config);
    }
    
    public void testBootstrap() throws Exception {
        Clock clock;
        
        int cnt = 0;
        long startTime = System.currentTimeMillis();
        while(true) {
            clock = _clockFactory.next();
            _retention.put(nextEvent(clock));
            cnt++;
            
            long elapsedTime = System.currentTimeMillis() - startTime;
            if(elapsedTime > 5000) {
                break;
            }
        }
        
        double rate;
        Chronos c = new Chronos();
        
        // Bootstrap
        int cnt2 = 0;
        Position pos = _retention.getPosition(Clock.ZERO);
        List<Event<String>> list = new ArrayList<Event<String>>();
        do {
            list.clear();
            pos = _retention.get(pos, list);
            cnt2 += list.size();
        } while(list.size() > 0);
        
        assertEquals(cnt, cnt2);
        
        rate = cnt2 / (double)c.tick();
        System.out.printf("%10.2f Events per ms, #Events=%d (Scan to Bootstrap)%n", rate, cnt2);
    }
}
