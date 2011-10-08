package krati.retention.clock;

import java.util.Iterator;

/**
 * WaterMarksClock
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/16, 2011 - Created <br/>
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
     * Sets the high water mark of a given source.
     * This method is similar to {{@link #updateHWMark(String, long)}
     * except that it does not return the current clock.
     * 
     * @param source - the data source
     * @param hwm    - the high water mark
     */
    public void setHWMark(String source, long hwm);
    
    /**
     * Updates the high water mark of a given source.
     * 
     * @param source - the data source
     * @param hwm    - the high water mark
     * @return the current clock. 
     */
    public Clock updateHWMark(String source, long hwm);
    
    /**
     * Updates the water marks of a given source.
     * 
     * @param source - the data source
     * @param lwm    - the low water mark
     * @param hwm    - the high water mark
     * @return the current clock.
     */
    public Clock updateWaterMarks(String source, long lwm, long hwm);
    
    /**
     * Sync the water marks of one source for persistency.
     * 
     * @return the current clock.
     */
    public Clock syncWaterMarks(String source);
    
    /**
     * Sync the water marks of all sources for persistency.
     * 
     * @return the current clock.
     */
    public Clock syncWaterMarks();
    
    /**
     * Flushes low water marks and high water marks for all the sources.
     * 
     * @return <tt>true</tt> if flush is successful.
     */
    public boolean flush();
    
    /**
     * Gets the water mark of a source from the specified clock.
     * 
     * @param source - the data source
     * @param clock  - the clock
     * @return the water mark of the specified source
     */
    public long getWaterMark(String source, Clock clock);
}
