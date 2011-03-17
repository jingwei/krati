package krati.core.array.entry;

import java.io.IOException;

import krati.io.DataWriter;

/**
 * EntryValueInt.
 * 
 * @author jwu
 */
public class EntryValueInt extends EntryValue {
    public int val;
    
    public EntryValueInt(int pos, int val, long scn) {
        super(pos, scn);
        this.val = val;
    }
    
    public final void reinit(int pos, int val, long scn) {
        this.pos = pos;
        this.val = val;
        this.scn = scn;
    }
    
    public final int getValue() {
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

        if (o instanceof EntryValueInt) {
            EntryValueInt v = (EntryValueInt) o;
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
        writer.writeInt(pos);  /* array position */
        writer.writeInt(val);  /* data value */
        writer.writeLong(scn); /* SCN value */
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
        writer.writeInt(position, val);
    }
}
