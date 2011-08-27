package krati.retention;

import java.io.IOException;

/**
 * RetentionFlushListener
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/14, 2011 - Created
 */
public interface RetentionFlushListener {
    
    /**
     * This method is called before an EventBatch is flushed. 
     */
    public void beforeFlush(EventBatch<?> batch) throws IOException;
    
    /**
     * This method is called after an EventBatch has been flushed. 
     */
    public void afterFlush(EventBatch<?> batch) throws IOException;
}
