package krati.cds;

import java.io.IOException;

/**
 * Persistable
 * 
 * @author jwu
 *
 */
public interface Persistable
{
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
    
    public long getLWMark();
    
    public long getHWMark();
    
    public void saveHWMark(long endOfPeriod) throws Exception;
}
