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

package test.retention;

import krati.io.Serializer;
import krati.io.serializer.StringSerializer;
import krati.retention.Retention;
import krati.retention.Event;
import krati.retention.SimpleRetention;
import krati.retention.SimpleEvent;
import krati.retention.clock.Clock;
import krati.retention.clock.Occurred;
import krati.retention.policy.RetentionPolicy;
import krati.retention.policy.RetentionPolicyOnSize;

/**
 * TestSimpleRetentionOnSize
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/09, 2011 - Created
 */
public class TestSimpleRetentionOnSize extends AbstractTestRetention<String> {
    
    @Override
    protected RetentionPolicy createRetentionPolicy() {
        return new RetentionPolicyOnSize(getNumRetentionBatches());
    }
    
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
        return new SimpleRetention<String>(
                getId(),
                getHomeDir(),
                createRetentionPolicy(),
                createBatchSerializer(),
                getEventBatchSize(),1);
    }
    
    public void testRetentionPolicy() throws Exception {
        Clock clock;
        Clock startClock;
        
        clock = _clockFactory.next();
        startClock = clock;
        _retention.put(nextEvent(clock));
        assertTrue(_retention.getMinClock().compareTo(startClock) == Occurred.EQUICONCURRENTLY);
        
        long startTime = System.currentTimeMillis();
        int cnt = getEventBatchSize() * getNumRetentionBatches() * 2;
        for(int i = 0; i < cnt; i++) {
            clock = _clockFactory.next();
            _retention.put(nextEvent(clock));
        }
        
        double rate = cnt / (double)(System.currentTimeMillis() - startTime);
        
        int sleepCnt = 10;
        while(_retention.getMinClock().compareTo(startClock) == Occurred.EQUICONCURRENTLY) {
            Thread.sleep(1000);
            if(--sleepCnt == 0) {
                break;
            }
        }
        
        assertTrue(_retention.getMinClock().after(startClock));
        
        System.out.printf("%10.2f Events per ms, #Events=%d (Populate)%n", rate, cnt);
    }
}
