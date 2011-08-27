package krati.retention;

/**
 * EventBatchCursor
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 07/31, 2011 - Created
 */
public interface EventBatchCursor {
    
    public int getLookup();
    
    public EventBatchHeader getHeader();
    
}
