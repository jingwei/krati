package test.retention;

import java.util.concurrent.TimeUnit;

import krati.retention.clock.Clock;
import krati.retention.policy.RetentionPolicy;
import krati.retention.policy.RetentionPolicyOnTime;

/**
 * TestSimpleRetentionOnTime
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/16, 2011 - Created
 */
public class TestSimpleRetentionOnTime extends TestSimpleRetentionOnSize {
    
    protected long getRetentionTimeInSeconds() {
        return 10;
    }
    
    @Override
    protected RetentionPolicy createRetentionPolicy() {
        return new RetentionPolicyOnTime(getRetentionTimeInSeconds(), TimeUnit.SECONDS);
    }
    
    @Override
    public void testRetentionPolicy() throws Exception {
        Clock clock;
        Clock startClock;
        
        clock = _clockFactory.next();
        startClock = clock;
        _retention.put(nextEvent(clock));
        
        assertTrue(_retention.getMinClock().compareTo(startClock) == 0);
        
        int cnt = 1;
        long startTime = System.currentTimeMillis();
        int runSeconds = (int)getRetentionTimeInSeconds() + 2;
        while(true) {
            long seconds = (System.currentTimeMillis() - startTime) / 1000;
            clock = _clockFactory.next();
            _retention.put(nextEvent(clock));
            cnt++;
            if(seconds >= runSeconds) {
                break;
            }
        }
        
        assertTrue(_retention.getMinClock().compareTo(startClock) > 0);
        
        double rate = cnt / (double)(System.currentTimeMillis() - startTime);
        System.out.printf("%10.2f Events per ms, #Events=%d (Populate)%n", rate, cnt);
    }
}
