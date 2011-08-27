package krati.retention.clock;

import java.io.Serializable;
import java.nio.ByteBuffer;


/**
 * Clock - A vector clock of long values.
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/11, 2011 - Created
 */
public class Clock implements Serializable, Comparable<Clock> {
    private final static long serialVersionUID = 1L;
    private final long[] _values;
    
    /**
     * The smallest Clock.
     */
    public static final Clock ZERO = new Clock();
    
    /**
     * Constructs a new instance of Clock.
     * 
     * @param values - a long array representing this Clock.
     */
    public Clock(long... values) {
        this._values = values;
    }
    
    /**
     * @return a long array representing this Clock.
     */
    public long[] values() {
        return _values;
    }
    
    /**
     * Parses a string representation of Clock.
     *  
     * @param str - A string representation of Clock
     * @return A Clock object 
     */
    public static Clock parseClock(String str) {
        String[] parts = str.split(":");
        long[] values = new long[parts.length];
        for(int i = 0; i < values.length; i++) {
            values[i] = Long.parseLong(parts[i]);
        }
        return new Clock(values);
    }
    
    public static Clock parseClock(byte[] raw) {
        if(raw == null || raw.length < 8) {
            return Clock.ZERO;
        }
        
        int cnt = raw.length >> 3;
        long[] values = new long[cnt];
        ByteBuffer bb = ByteBuffer.wrap(raw);
        for(int i = 0; i < values.length; i++) {
            values[i] = bb.getLong();
        }
        return new Clock(values);
    }
    
    /**
     * Gets the string representation of Clock in a colon separated list, e.g. <tt>16335:16912:15999</tt>.
     * 
     * @return a string representation of Clock
     */
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        if(_values != null && 1 <= _values.length) {
            b.append(_values[0]);
            for(int i = 1; i < _values.length; i++) {
                b.append(':').append(_values[i]);
            }
        }
        return b.toString();
    }
    
    /**
     * @return a byte array representing this Clock.
     */
    public byte[] toByteArray() {
        if(_values != null) {
            byte[] byteArray = new byte[_values.length << 3];
            ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
            for(int i = 0; i < _values.length; i++) {
                byteBuffer.putLong(_values[i]);
            }
            return byteArray;
        } else {
            return new byte[0];
        }
    }
    
    /**
     * Compares this clock with the specified clock for order.
     * Returns a negative integer, zero, or a positive integer as this clock is less than, equal to, or greater than the specified clock.
     * 
     * @param c - a clock to compare.
     */
    @Override
    public int compareTo(Clock c) {
        if(this == c) return 0;
        if(ZERO == c) return 1;
        if(this == ZERO) return -1;
        
        int neg = 0, pos = 0, eq = 0;
        try {
            final long[] dst = c.values();
            final int len = dst.length;
            if(_values.length == len) {
                for(int i = 0; i < len; i++) {
                    long cmp = _values[i] - dst[i];
                    if(cmp < 0) {
                        neg++;
                    } else if(cmp > 0) {
                        pos++;
                    } else {
                        eq++;
                    }
                }
                
                if(neg == len) {
                    return -1;
                } else if(eq == len) {
                    return 0;
                } else if(pos == len) {
                    return 1;
                } else {
                    neg += eq;
                    if(neg == len) {
                        return -1;
                    }
                    
                    pos += eq;
                    if(pos == len) {
                        return 1;
                    }
                }
            }
        } catch(Exception e) {}
        
        throw new IncomparableClocksException(this, c);
    }
}
