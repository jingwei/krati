package krati.retention.clock;

/**
 * ClockWatcher
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/18, 2011 - Created
 */
public interface ClockWatcher {

    /**
     * @return the current clock.
     */
    public Clock current();
}
