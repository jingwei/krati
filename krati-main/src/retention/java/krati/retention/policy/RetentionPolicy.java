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

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import krati.retention.EventBatch;
import krati.retention.EventBatchCursor;

/**
 * RetentionPolicy
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/08, 2011 - Created
 */
public interface RetentionPolicy {

    /**
     * Apply this RetentionPolicy on a concurrent queue of EventBatchCursor(s) to discard cursors not to be retained.
     * 
     * <p>
     * The classes implementing RetentionPolicy can peek/poll from the concurrent <tt>queue</tt>.
     * An EventBatchCursor is dequeued from the <tt>queue</tt> only when it is discarded by the policy.
     * 
     * @param queue - the concurrent queue of EventBatchCursor(s)
     * @return A collection of EventBatchCursor(s) discarded by this policy.
     *         Any EventBatchCursor in the resulting collection should be removed from the <tt>queue</tt>. 
     */
    public Collection<EventBatchCursor> apply(ConcurrentLinkedQueue<EventBatchCursor> queue);
    
    /**
     * Tests if this RetentionPolicy is a callback.
     * 
     * @return <tt>true</tt> if this RetentionPolicy need to post-process EventBatch(s) for discarded EventBatchCursor(s).
     */
    public boolean isCallback();
    
    /**
     * Apply callback on a discarded EventBatch.
     * 
     * @param batch - the EventBatch to be processed by the callback.
     */
    public void applyCallbackOn(EventBatch<?> batch);
    
}
