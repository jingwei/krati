package krati.retention.policy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import krati.retention.EventBatch;
import krati.retention.EventBatchCursor;

/**
 * RetentionPolicyOnSize
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/08, 2011 - Created
 */
public class RetentionPolicyOnSize implements RetentionPolicy {
    private int _numRetentionBatches;
    
    /**
     * Constructs a retention policy managing up to 1000 retention batches.
     */
    public RetentionPolicyOnSize() {
        this(1000);
    }
    
    /**
     * Constructs a retention policy with the given <tt>numRetentionBatches</tt>.
     * 
     * @param numRetentionBatches
     */
    public RetentionPolicyOnSize(int numRetentionBatches) {
        setNumRetentionBatches(numRetentionBatches);
    }
    
    public final int getNumRetentionBatches() {
        return _numRetentionBatches;
    }
    
    public final void setNumRetentionBatches(int numRetentionBatches) {
        this._numRetentionBatches = Math.max(1, numRetentionBatches);
    }
    
    @Override
    public synchronized Collection<EventBatchCursor> apply(ConcurrentLinkedQueue<EventBatchCursor> queue) {
        ArrayList<EventBatchCursor> results = new ArrayList<EventBatchCursor>();
        
        while(queue.size() > getNumRetentionBatches()) {
            EventBatchCursor c = queue.poll();
            if(c != null) {
                results.add(c);
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
