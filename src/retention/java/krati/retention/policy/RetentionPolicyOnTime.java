package krati.retention.policy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import krati.retention.EventBatch;
import krati.retention.EventBatchCursor;

/**
 * RetentionPolicyOnTime
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/14, 2011 - Created
 */
public class RetentionPolicyOnTime implements RetentionPolicy {
    public final static long MIN_DURATION_MILLIS = 1000;
    
    private long _duration;
    private TimeUnit _timeUnit;
    private long _timeMillis = MIN_DURATION_MILLIS;
    
    public RetentionPolicyOnTime(long duration, TimeUnit timeUnit) {
        setRetention(duration, timeUnit);
    }
    
    public final long getDuration() {
        return _duration;
    }
    
    public final TimeUnit getTimeUnit() {
        return _timeUnit;
    }
    
    public final long getTimeMillis() {
        return _timeMillis;
    }
    
    public void setRetention(long duration, TimeUnit timeUnit) {
        this._duration = duration;
        this._timeUnit = timeUnit;
        
        long millis = TimeUnit.MILLISECONDS == timeUnit ?
                duration : TimeUnit.MILLISECONDS.convert(duration, timeUnit);
        _timeMillis = Math.max(millis, MIN_DURATION_MILLIS);
    }
    
    @Override
    public Collection<EventBatchCursor> apply(ConcurrentLinkedQueue<EventBatchCursor> queue) {
        ArrayList<EventBatchCursor> results = new ArrayList<EventBatchCursor>();
        
        while(queue.size() > 0) {
            EventBatchCursor c = queue.peek();
            if(c != null) {
                long diff = System.currentTimeMillis() - c.getHeader().getCompletionTime();
                if(diff > _timeMillis) {
                    c = queue.poll();
                    results.add(c);
                } else {
                    break;
                }
            }
        }
        
        return results;
    }
    
    @Override
    public void applyCallbackOn(EventBatch<?> batch) {}
    
    @Override
    public boolean isCallback() {
        return false;
    }
}
