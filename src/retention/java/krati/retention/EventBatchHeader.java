package krati.retention;

import krati.retention.clock.Clock;

/**
 * EventBatchHeader
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 07/31, 2011 - Created
 */
public interface EventBatchHeader {
    
    public int getVersion();
    
    public int getSize();
    
    public long getOrigin();
    
    public long getCreationTime();
    
    public long getCompletionTime();
    
    public Clock getMinClock();
    
    public Clock getMaxClock();
}
