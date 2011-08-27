package test.retention;

import krati.io.Serializer;
import krati.io.serializer.StringSerializer;
import krati.retention.Retention;
import krati.retention.Event;
import krati.retention.SimpleRetention;
import krati.retention.SimpleEvent;
import krati.retention.clock.Clock;
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
                getEventBatchSize());
    }
    
    public void testRetentionPolicy() throws Exception {
        Clock clock;
        Clock startClock;
        
        clock = _clockFactory.next();
        startClock = clock;
        _retention.put(nextEvent(clock));
        assertTrue(_retention.getMinClock().compareTo(startClock) == 0);
        
        long startTime = System.currentTimeMillis();
        int cnt = getEventBatchSize() * getNumRetentionBatches() * 2;
        for(int i = 0; i < cnt; i++) {
            clock = _clockFactory.next();
            _retention.put(nextEvent(clock));
        }
        
        double rate = cnt / (double)(System.currentTimeMillis() - startTime);
        
        int sleepCnt = 10;
        while(_retention.getMinClock().compareTo(startClock) == 0) {
            Thread.sleep(1000);
            if(--sleepCnt == 0) {
                break;
            }
        }
        
        assertTrue(_retention.getMinClock().compareTo(startClock) > 0);
        
        System.out.printf("%10.2f Events per ms, #Events=%d (Populate)%n", rate, cnt);
    }
}
