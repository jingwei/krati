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
