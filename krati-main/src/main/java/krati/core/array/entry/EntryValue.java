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
 * EntryValue.
 * 
 * @author jwu
 */
public abstract class EntryValue implements Comparable<EntryValue> {
    public int pos; // position in the array
    public long scn; // SCN associated with an update at the position
    
    public EntryValue(int pos, long scn) {
        this.pos = pos;
        this.scn = scn;
    }
    
    public final int getPosition() {
        return pos;
    }
    
    public final long getScn() {
        return scn;
    }
    
    @Override
    public String toString() {
        return pos + ":?:" + scn;
    }
    
    public int compareTo(EntryValue o) {
        return (pos < o.pos ? -1 : (pos == o.pos ? (scn < o.scn ? -1 : (scn == o.scn ? 0 : 1)) : 1));
    }
    
    /**
     * Writes this EntryValue to entry log file via a channel writer.
     * 
     * @param writer
     * @throws IOException
     */
    public abstract void write(DataWriter writer) throws IOException;
    
    /**
     * Writes this EntryValue to a file channel at a given position.
     * 
     * @param writer
     * @param position
     * @throws IOException
     */
    public abstract void updateArrayFile(DataWriter writer, long position) throws IOException;
    
}
