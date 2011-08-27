package krati.retention;

import java.io.Serializable;

import krati.retention.clock.Clock;

/**
 * Event
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 07/28, 2011 - Created
 */
public interface Event<T> extends Serializable {
    
    public T getValue();
    
    public Clock getClock();
}
