package krati.retention.clock;

import java.util.Iterator;

/**
 * WaterMarksClock
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/16, 2011 - Created
 */
public interface WaterMarksClock extends ClockWatcher {
    
    /**
     * Tests if this WaterMarksClock has a user specified source.
     * 
     * @param source - data source
     */
    public boolean hasSource(String source);
    
    /**
     * @return an iterator of sources known to this WaterMarksClock.
     */
    public Iterator<String> sourceIterator();
    
    /**
     * @return the current clock of this WaterMarksClock.
     */
    public Clock current();
    
    /**
     * Gets the low water mark of a given source.
     * 
     * @param source - the data source
     * @return the low water mark
     */
    public long getLWMScn(String source);
    
    /**
     * Gets the high water mark of a given source.
     * 
     * @param source - the data source
     * @return the high water mark
     */
    public long getHWMScn(String source);
    
    /**
     * Save the high water mark of a given source.
     * 
     * @param source - the data source
     * @param hwm    - the high water mark
     */
    public Clock saveHWMark(String source, long hwm);
    
    /**
     * Resets the water marks for a given source.
     * 
     * @param source - the data source
     * @param lwm    - the low water mark
     * @param hwm    - the high water mark
     */
    public Clock setWaterMarks(String source, long lwm, long hwm);
    
    /**
     * Sync the water marks of one source for persistency.
     */
    public Clock syncWaterMarks(String source);
    
    /**
     * Sync the water marks of all sources for persistency.
     */
    public Clock syncWaterMarks();
}
