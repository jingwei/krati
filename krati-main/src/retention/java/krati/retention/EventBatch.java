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

package krati.retention;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import krati.retention.clock.Clock;

/**
 * EventBatch
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 07/31, 2011 - Created <br/>
 * 04/19, 2012 - Reduced MINIMUM_BATCH_SIZE to 100 <br/>
 */
public interface EventBatch<T> extends Iterable<Event<T>>, EventBatchHeader, Serializable {
    public static final int VERSION = 0;
    public static final int MINIMUM_BATCH_SIZE = 100;
    public static final int DEFAULT_BATCH_SIZE = 10000;
    
    /**
     * @return the current header of this EventBatch.
     */
    public EventBatchHeader getHeader();
    
    /**
     * Sets the creation time of this EventBatch.
     * @param time - Time measured in milliseconds, between the current time and midnight, January 1, 1970 UTC.
     */
    public void setCreationTime(long time);
    
    /**
     * Sets the completion time of this EventBatch.
     * @param time - Time measured in milliseconds, between the current time and midnight, January 1, 1970 UTC.
     */
    public void setCompletionTime(long time);
    
    /**
     * Gets the clock associated with an offset.
     * 
     * @param offset
     * @return <tt>null</tt> if the offset is not in this batch.
     */
    public Clock getClock(long offset);
    
    /**
     * Gets the offset of the first event that occurred at <tt>sinceClock</tt>
     * or the offset of an event that occurred right before <tt>sinceClock</tt>.
     * 
     * @param sinceClock - the sinceClock
     * @return <tt>-1</tt> if the <tt>sinceClock</tt> is not known to this batch.
     */
    public long getOffset(Clock sinceClock);
    
    /**
     * Gets events starting at an offset.
     * 
     * @param offset - the offset
     * @param list   - the event list to fill in
     * @return a list of events
     */
    public long get(long offset, List<Event<T>> list);
    
    /**
     * Gets events starting at an offset.
     * 
     * @param offset - the offset
     * @param count  - the number of events
     * @param list   - the event list to fill in
     * @return the next offset
     */
    public long get(long offset, int count, List<Event<T>> list);
    
    /**
     * Puts an event into this EventBatch
     * 
     * @param event
     * @return <tt>true</tt> if the <tt>event</tt> is added to this batch. Otherwise, <tt>false</tt>.
     */
    public boolean put(Event<T> event);
    
    /**
     * @return an iterator of events owned by this EventBatch. 
     */
    @Override
    public Iterator<Event<T>> iterator();
    
    /**
     * @return <code>true</code> if this EventBatch is empty. Otherwise, <code>false</code>.
     */
    public boolean isEmpty();
    
    /**
     * @return <code>true</code> if this EventBatch is full. Otherwise, <code>false</code>.
     */
    public boolean isFull();
}
