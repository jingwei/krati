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

import java.io.IOException;
import java.util.List;

import krati.io.Closeable;
import krati.retention.clock.Clock;
import krati.retention.policy.RetentionPolicy;

/**
 * Retention
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 07/28, 2011 - Created
 */
public interface Retention<T> extends Closeable, RetentionClient<T> {
    
    /**
     * @return the unique Id.
     */
    public int getId();
    
    /**
     * @return the origin offset.
     */
    public long getOrigin();
    
    /**
     * @return the current offset.
     */
    public long getOffset();
    
    /**
     * @return the min clock known to the retention period of Retention.
     */
    public Clock getMinClock();
    
    /**
     * @return the max clock known to the retention period of Retention.
     */
    public Clock getMaxClock();
    
    /**
     * Gets the clock associated with a given Retention offset.
     * 
     * @param offset - the retention offset
     * @return The clock associated with <tt>offset</tt> or
     * <tt>null</tt> if <tt>offset</tt> is not in the retention period.
     */
    public Clock getClock(long offset);
    
    /**
     * @return the event batch size.
     */
    public int getBatchSize();
    
    /**
     * @return the retention policy.
     */
    public RetentionPolicy getRetentionPolicy();
    
    /**
     * @return the current Retention position.
     */
    @Override
    public Position getPosition();
    
    /**
     * Gets the position of the first event that occurred at <tt>sinceClock</tt>
     * or the position of an event that occurred right before <tt>sinceClock</tt>.
     * 
     * @param sinceClock - the since Clock.
     * @return Position <tt>null</tt> if the first event at <tt>sinceClock</tt>
     * is removed from retention (i.e. out of the retention period).
     */
    @Override
    public Position getPosition(Clock sinceClock);
    
    /**
     * Gets a number of events starting from a give position in the Retention.
     * The number of events is determined internally by the Retention and it is
     * up to the batch size.   
     * 
     * @param pos  - the retention position from where events will be read
     * @param list - the event list to fill in
     * @return the next position from where new events will be read. 
     * @throws NullPointerException if any argument is <tt>null</tt>.
     * @throws InvalidPositionException if <tt>pos</tt> refers to a position in bootstrap
     *         that originates from a different retention replica.
     * @throws OperationAbortedException if bootstrap takes longer than what is allowed
     *         by this retention.
     */
    @Override
    public Position get(Position pos, List<Event<T>> list);
    
    /**
     * Puts a new event into the Retention.
     * 
     * @param event
     * @return <tt>true</tt> if the event is added to the Retention.
     * @throws Exception if the operation cannot be completed for any reason.
     */
    public boolean put(Event<T> event) throws Exception;
    
    /**
     * Flushes update events contained by this Retention.
     * 
     * @throws IOException
     */
    public void flush() throws IOException;
}
