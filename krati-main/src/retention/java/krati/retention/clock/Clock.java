/*
 * Copyright (c) 2010-2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package krati.retention.clock;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Clock - A vector clock of long values.
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/11, 2011 - Created <br/>
 * 09/27, 2011 - Updated compareTo to return Occurred <br/>
 */
public final class Clock implements Serializable {
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
     * Parses a Clock value from its string representation.
     *  
     * @param str - the string representation of Clock
     * @return A Clock object.
     * <code>Clock.ZERO</code> is returned <code>upon</code> null or zero-length string.  
     */
    public static Clock parseClock(String str) {
        if(str == null || str.length() == 0) {
            return Clock.ZERO;
        }
        
        String[] parts = str.split(":");
        long[] values = new long[parts.length];
        for(int i = 0; i < values.length; i++) {
            values[i] = Long.parseLong(parts[i]);
        }
        return new Clock(values);
    }
    
    /**
     * Parses a Clock value from its raw bytes.
     * 
     * @param raw - the raw bytes of Clock
     * @return a Clock object.
     * <code>Clock.ZERO</code> is returned upon <code>null</code> or a byte array with the length less than 8.
     */
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
     * Compares this clock with the specified clock for ordering.
     * 
     * @param c - a clock to compare.
     */
    public Occurred compareTo(Clock c) {
        if(this == c) return Occurred.EQUICONCURRENTLY;
        if(ZERO == c) return Occurred.AFTER;
        if(this == ZERO) return Occurred.BEFORE;
        
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
                
                if(eq == len) {
                    return Occurred.EQUICONCURRENTLY;
                } else if(neg == len) {
                    return Occurred.BEFORE;
                } else if(pos == len) {
                    return Occurred.AFTER;
                } else {
                    neg += eq;
                    if(neg == len) {
                        return Occurred.BEFORE;
                    }
                    
                    pos += eq;
                    if(pos == len) {
                        return Occurred.AFTER;
                    }
                    
                    return Occurred.CONCURRENTLY;
                }
            }
        } catch(Exception e) {}
        
        throw new IncomparableClocksException(this, c);
    }
    
    /**
     * @return <code>true</code> if this Clock occurred before the specified Clock <code>c</code>.
     *         Otherwise, <code>false</code>.
     */
    public boolean before(Clock c) {
        return compareTo(c) == Occurred.BEFORE;
    }
    
    /**
     * @return <code>true</code> if this Clock occurred after the specified Clock <code>c</code>.
     *         Otherwise, <code>false</code>.
     */
    public boolean after(Clock c) {
        return compareTo(c) == Occurred.AFTER;
    }
    
    /**
     * @return <code>true</code> if this Clock is equal to or occurred before the specified Clock <code>c</code>.
     *         Otherwise, <code>false</code>.
     */
    public boolean beforeEqual(Clock c) {
        Occurred o = compareTo(c);
        return o == Occurred.BEFORE || o == Occurred.EQUICONCURRENTLY;
    }
    
    /**
     * @return <code>true</code> if this Clock is equal to or occurred after the specified Clock <code>c</code>.
     *         Otherwise, <code>false</code>.
     */
    public boolean afterEqual(Clock c) {
        Occurred o = compareTo(c);
        return o == Occurred.AFTER || o == Occurred.EQUICONCURRENTLY; 
    }
    
    @Override
    public boolean equals(Object o) {
        if (null == o) return false;
        if (o.getClass() != this.getClass()) return false;
        Clock c = (Clock) o;
        return Arrays.equals(this._values, c._values);
    }
    
    @Override
    public int hashCode() {
        return Arrays.hashCode(_values);
    }
}
