package krati.retention;

import java.util.List;

import krati.retention.clock.Clock;

/**
 * RetentionClient
 * 
 * @param <T> Event Value
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/16, 2011 - Created <br/>
 */
public interface RetentionClient<T> {
    
    /**
     * @return the current position in Retention.
     */
    public Position getPosition();
    
    /**
     * Gets the position of the first event that occurred at <tt>sinceClock</tt>
     * or the position of an event that occurred right before <tt>sinceClock</tt>.
     * 
     * @param sinceClock - the since Clock.
     * @return Position <tt>null</tt> if the first event at <tt>sinceClock</tt>
     * is removed from retention (i.e. out of the retention period).
     */
    public Position getPosition(Clock sinceClock);
    
    /**
     * Gets a number of events starting from a give position in the Retention.
     * The number of events is determined internally by the Retention and it is
     * up to the batch size.   
     * 
     * @param pos  - the retention position from where events will be read
     * @param list - the event list to fill in
     * @return the next position from where new events will be read. 
     */
    public Position get(Position pos, List<Event<T>> list);
    
}
