package krati.retention;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import krati.retention.clock.Clock;

/**
 * SimpleEventBatch
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/01, 2011 - Created
 */
public final class SimpleEventBatch<T> implements EventBatch<T> {
    private static final long serialVersionUID = 1L;
    private final long _origin;
    private final int _capacity;
    private volatile Clock _minClock;
    private volatile Clock _maxClock;
    private volatile long _creationTime;
    private volatile long _completionTime;
    private final ArrayList<Event<T>> _events;
    
    public SimpleEventBatch(long origin, Clock initClock) {
        this(origin, initClock, EventBatch.DEFAULT_BATCH_SIZE);
    }
    
    public SimpleEventBatch(long origin, Clock initClock, int capacity) {
        this._origin = origin;
        this._minClock = initClock;
        this._maxClock = initClock;
        this._capacity = Math.max(EventBatch.MINIMUM_BATCH_SIZE, capacity);
        this._events = new ArrayList<Event<T>>(this._capacity);
        this._creationTime = System.currentTimeMillis();
        this._completionTime = System.currentTimeMillis();
    }
    
    @Override
    public int getVersion() {
        return EventBatch.VERSION;
    }
    
    @Override
    public int getSize() {
        return _events.size();
    }
    
    @Override
    public long getOrigin() {
        return _origin;
    }
    
    @Override
    public long getCreationTime() {
        return _creationTime;
    }
    
    @Override
    public void setCreationTime(long time) {
        this._creationTime = time;
    }
    
    @Override
    public long getCompletionTime() {
        return _completionTime;
    }
    
    @Override
    public void setCompletionTime(long time) {
        this._completionTime = time;
    }
    
    @Override
    public Clock getMinClock() {
        return _minClock;
    }
    
    @Override
    public Clock getMaxClock() {
        return _maxClock;
    }
    
    @Override
    public Iterator<Event<T>> iterator() {
        return _events.iterator();
    }
    
    @Override
    public boolean isEmpty() {
        return _events.isEmpty();
    }
    
    @Override
    public boolean isFull() {
        return _events.size() >= _capacity;
    }
    
    @Override
    public EventBatchHeader getHeader() {
        return new SimpleEventBatchHeader(
                getVersion(),
                getSize(),
                getOrigin(),
                getCreationTime(),
                getCompletionTime(),
                getMinClock(), getMaxClock());
    }
    
    @Override
    public Clock getClock(long offset) {
        if(_origin <= offset && offset < (_origin + _events.size())) {
            return _events.get((int)(offset - _origin)).getClock();
        }
        
        return null;
    }
    
    @Override
    public long getOffset(Clock sinceClock) {
        if(_minClock.compareTo(sinceClock) < 0 && sinceClock.compareTo(_maxClock) <= 0) {
            int i = 0;
            for(; i < _events.size(); i++) {
                Event<T> e = _events.get(i);
                int cmp = sinceClock.compareTo(e.getClock());
                if(cmp == 0) {
                    break;
                } else if(cmp < 0) {
                    i--;
                    break;
                }
            }
            return _origin + i;
        }
        
        return -1;
    }
    
    @Override
    public boolean put(Event<T> event) {
        Clock clock = event.getClock();
        int size = _events.size();
        
        if(size == 0 && _minClock.compareTo(clock) < 0) {
            _minClock = clock;
            _maxClock = clock;
        }
        
        if(size < _capacity && _maxClock.compareTo(clock) <= 0) {
            _events.add(event);
            _maxClock = clock;
            return true;
        }
        
        return false;
    }
    
    @Override
    public long get(long offset, List<Event<T>> list) {
        return get(offset, Integer.MAX_VALUE, list);
    }
    
    @Override
    public long get(long offset, int count, List<Event<T>> list) {
        int ind = (int)(offset - getOrigin());
        if(0 <= ind && ind < _events.size()) {
            for(; ind < _events.size(); ind++) {
                Event<T> e = _events.get(ind);
                list.add(e);
                
                count--;
                if(0 == count) {
                    return getOrigin() + ind + 1;
                }
            }
            return getOrigin() + ind;
        }
        
        return offset;
    }
    
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append(SimpleEventBatch.class.getSimpleName()).append("{");
        b.append("version=").append(getVersion()).append(",");
        b.append("size=").append(_events.size()).append(",");
        b.append("origin=").append(_origin).append(",");
        b.append("creationTime=").append(_creationTime).append(",");
        b.append("completionTime=").append(_completionTime).append(",");
        b.append("minClock=").append(_minClock).append(",");
        b.append("maxClock=").append(_maxClock).append("}");
        return b.toString();
    }
}
