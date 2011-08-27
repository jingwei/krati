package krati.retention;

import krati.retention.clock.Clock;

/**
 * SimplePosition
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/02, 2011 - Created
 */
public class SimplePosition implements Position {
    private static final long serialVersionUID = 1L;
    private int _id = -1;
    private long _offset = -1;
    private int _index = -1;
    private Clock _clock;
    
    /**
     * Creates a SimplePosition for real-time update.
     * 
     * @param id     - the Id
     * @param offset - the offset
     * @param clock  - the global clock
     */
    public SimplePosition(int id, long offset, Clock clock) {
        this(id, offset, -1, clock);
    }
    
    /**
     * Creates a SimplePosition for grand fathering.
     * 
     * @param id     - the Id
     * @param offset - the offset
     * @param index  - the index
     * @param clock  - the global clock
     */
    public SimplePosition(int id, long offset, int index, Clock clock) {
        this._id = id;
        this._offset = offset;
        this._index = index;
        this._clock = clock;
    }
    
    @Override
    public int getId() {
        return _id;
    }
    
    @Override
    public long getOffset() {
        return _offset;
    }
    
    @Override
    public int getIndex() {
        return _index;
    }
    
    @Override
    public boolean isIndexed() {
        return (_index >= 0);
    }
    
    @Override
    public Clock getClock() {
        return _clock;
    }
    
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append(_id).append(':')
         .append(_offset).append(':')
         .append(_index).append(':')
         .append(_clock);
        return b.toString();
    }
    
    /**
     * Parses a string representation of <tt>SimplePosition</tt> in the form of <tt>Id:Offset:Index:Clock</tt>.
     * 
     * <p>
     * For example, <tt>1:128956235:59272:12789234:12789257:12789305</tt> defines a position with <tt>Id=1</tt>,
     * <tt>Offset=128956235</tt>, <tt>Index=59272</tt> and <tt>Clock=12789234:12789257:12789305</tt>.
     * 
     * @param s - the string representation of a position.
     * @throws <tt>NullPointerException</tt> if the string <tt>s</tt> is null.
     * @throws <tt>NumberFormatException</tt> if the string <tt>s</tt> contains non-parsable <tt>int</tt> or <tt>long</tt>.   
     */
    public static SimplePosition parsePosition(String s) {
        String[] parts = s.split(":");
        int id = Integer.parseInt(parts[0]);
        long offset = Long.parseLong(parts[1]);
        int index = Integer.parseInt(parts[2]);
        
        if(parts.length == 3) {
            return new SimplePosition(id, offset, index, Clock.ZERO);
        }
        
        long[] values = new long[parts.length - 3];
        for(int i = 0; i < values.length; i++) {
            values[i] = Long.parseLong(parts[3+i]);
        }
        
        return new SimplePosition(id, offset, index, new Clock(values));
    }
}
