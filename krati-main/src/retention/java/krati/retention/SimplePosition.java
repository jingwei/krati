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
    public final String toString() {
        StringBuilder b = new StringBuilder();
        b.append(_id).append(':')
         .append(_offset).append(':')
         .append(_index).append(':')
         .append(_clock);
        return b.toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (null == o) return false;
        if (o.getClass() != this.getClass()) return false;
        SimplePosition p = (SimplePosition) o;
        return p._id == this._id
            && p._offset == this._offset
            && p._index == this._index
            && p._clock.equals(this._clock);
    }
    
    @Override
    public int hashCode() {
        int hash = 5;
        hash = 97 * hash + _id;
        hash = 97 * hash + (int)_offset;
        hash = 97 * hash + _index;
        hash = 97 * hash + _clock.hashCode();
        return hash; 
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
