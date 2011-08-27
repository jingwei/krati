package krati.retention;

import krati.retention.clock.Clock;

/**
 * SimpleEvent
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 07/28, 2011 - Created
 */
public class SimpleEvent<T> implements Event<T> {
    private final static long serialVersionUID = 1L;
    private final Clock _clock;
    private final T _value;
    
    public SimpleEvent(T value, Clock clock) {
        this._value = value;
        this._clock = clock;
    }
    
    @Override
    public T getValue() {
        return _value;
    }
    
    @Override
    public Clock getClock() {
        return _clock;
    }
    
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName()).append("{");
        b.append("value=").append(_value).append(",");
        b.append("clock=").append(_clock).append("}");
        return b.toString();
    }
}
