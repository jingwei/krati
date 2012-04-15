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

package krati.core.array.entry;

import java.io.IOException;

import krati.io.DataWriter;

/**
 * EntryValueShort
 * 
 * @author jwu
 * 
 */
public class EntryValueShort extends EntryValue {
    public short val;
    
    public EntryValueShort(int pos, short val, long scn) {
        super(pos, scn);
        this.val = val;
    }
    
    public final void reinit(int pos, short val, long scn) {
        this.pos = pos;
        this.val = val;
        this.scn = scn;
    }
    
    public final short getValue() {
        return val;
    }
    
    @Override
    public String toString() {
        return pos + ":" + val + ":" + scn;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null)
            return false;
        
        if (o instanceof EntryValueShort) {
            EntryValueShort v = (EntryValueShort) o;
            return (pos == v.pos) && (val == v.val) && (scn == v.scn);
        } else {
            return false;
        }
    }
    
    @Override
    public int hashCode() {
        int result;
        result = pos / 29 + val / 113;
        result = 19 * result + (int) (scn ^ (scn >>> 32));
        return result;
    }
    
    /**
     * Writes this EntryValue to entry log file via a data writer.
     * 
     * @param writer
     * @throws IOException
     */
    @Override
    public void write(DataWriter writer) throws IOException {
        writer.writeInt(pos);   /* array position */
        writer.writeShort(val); /* data value */
        writer.writeLong(scn);  /* SCN value */
    }
    
    /**
     * Writes this EntryValue at a given position of a data writer.
     * 
     * @param writer
     * @param position
     * @throws IOException
     */
    @Override
    public void updateArrayFile(DataWriter writer, long position) throws IOException {
        writer.writeShort(position, val);
    }
}