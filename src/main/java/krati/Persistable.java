package krati;

import java.io.IOException;

/**
 * Persistable
 * 
 * @author jwu
 *
 */
public interface Persistable {
    /**
     * Force all updates from memory buffer and redo log files to synchronize with
     * the underlying persistent file in blocking mode.
     *  
     * @throws IOException
     */
    public void sync() throws IOException;
    
    /**
     * Persist all updates from memory buffer into redo log files in non-blocking mode.
     *  
     * @throws IOException
     */
    public void persist() throws IOException;
    
    /**
     * Gets the low water mark.
     */
    public long getLWMark();
    
    /**
     * Gets the high water mark.
     */
    public long getHWMark();
    
    /**
     * Save the high water mark.
     * 
     * @param endOfPeriod
     * @throws Exception
     */
    public void saveHWMark(long endOfPeriod) throws Exception;
}
