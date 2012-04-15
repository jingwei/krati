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
 * 08/14, 2011 - Created <br/>
 * 12/06, 2011 - Added a new constructor using minutes <br/>
 */
public class RetentionPolicyOnTime implements RetentionPolicy {
    public final static long MIN_DURATION_MILLIS = 1000;
    
    private long _duration;
    private TimeUnit _timeUnit;
    private long _timeMillis = MIN_DURATION_MILLIS;
    
    /**
     * Constructs a retention policy managing a 4-hour retention period.
     */
    public RetentionPolicyOnTime() {
        this(4, TimeUnit.HOURS);
    }
    
    /**
     * Constructs a retention policy managing the specified retention period.
     *
     * @param minutes - the retention duration in minutes (at least 1 minute)
     */
    public RetentionPolicyOnTime(int minutes) {
        this(Math.max(1, minutes), TimeUnit.MINUTES);
    }
    
    /**
     * Constructs a retention policy with the given <tt>duration</tt> and <tt>timeUnit</tt>.
     * 
     * @param duration - the retention duration
     * @param timeUnit - the time unit as defined by {@link TimeUnit}
     */
    public RetentionPolicyOnTime(long duration, TimeUnit timeUnit) {
        setRetention(Math.max(1, duration), timeUnit);
    }
    
    /**
     * Gets the retention duration.
     */
    public final long getDuration() {
        return _duration;
    }
    
    /**
     * Gets the retention time unit as defined by {@link TimeUnit}.
     */
    public final TimeUnit getTimeUnit() {
        return _timeUnit;
    }
    
    /**
     * Gets the retention time in milliseconds.
     */
    public final long getTimeMillis() {
        return _timeMillis;
    }
    
    /**
     * Sets the retention duration period of this policy.
     * 
     * @param duration - the retention duration
     * @param timeUnit - the time unit as defined by {@link TimeUnit}
     */
    public void setRetention(long duration, TimeUnit timeUnit) {
        this._duration = Math.max(1, duration);
        this._timeUnit = timeUnit;
        
        long millis = TimeUnit.MILLISECONDS == timeUnit ?
                _duration : TimeUnit.MILLISECONDS.convert(_duration, timeUnit);
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
