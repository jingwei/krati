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
     * Sync all updates with the underlying persistent file in blocking mode.
     *  
     * @throws IOException
     */
    public void sync() throws IOException;
    
    /**
     * Persist all updates into redo log files in non-blocking mode.
     *  
     * @throws IOException
     */
    public void persist() throws IOException;
    
    public long getLWMark();
    
    public long getHWMark();
    
    public void saveHWMark(long endOfPeriod) throws Exception;
}
