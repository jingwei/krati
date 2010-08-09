package krati.cds.impl.array.entry;

import java.io.IOException;

import krati.io.DataWriter;

public class EntryValueLongDual extends EntryValue
{
    public long val;
    public long valDual;
    
    public EntryValueLongDual(int pos, long val, long valDual, long scn)
    {
        super(pos, scn);
        this.val = val;
        this.valDual = valDual;
    }
    
    public final void reinit(int pos, long val, long valDual, long scn)
    {
        this.pos = pos;
        this.val = val;
        this.valDual = valDual;
        this.scn = scn;
    }

    public final long getValue()
    {
        return val;
    }
    
    public final long getValueDual()
    {
        return valDual;
    }
    
    @Override
    public String toString()
    {
        return pos + ":" + val + ":" + valDual + ":" + scn;
    }
    
    @Override
    public boolean equals(Object o)
    {
        if(this == o) return true;
        if(o == null) return false;
        
        if(o instanceof EntryValueLongDual)
        {
            EntryValueLongDual v = (EntryValueLongDual)o;
            return (pos == v.pos) && (val == v.val) && (valDual == v.valDual) && (scn == v.scn);
        }
        else
        {
            return false;
        }
    }
    
    @Override
    public int hashCode()
    {
        int result;
        result = pos/29 + (int) (val/113) + (int) (valDual/311);
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
    public void write(DataWriter writer) throws IOException
    {
        writer.writeInt(pos);      /* array position */
        writer.writeLong(val);     /* data value     */
        writer.writeLong(valDual); /* data value dual*/
        writer.writeLong(scn);     /* SCN value      */
    }
    
    /**
     * Writes this EntryValue at a given position of a data writer.
     * 
     * @param writer
     * @param position
     * @throws IOException
     */
    @Override
    public void updateArrayFile(DataWriter writer, long position) throws IOException
    {
        writer.writeLong(position, val);
    }
    
    /**
     * Writes this EntryValue at a given position of a data writer.
     * 
     * @param writer
     * @param position
     * @throws IOException
     */
    public void updateArrayFileDual(DataWriter writer, long position) throws IOException
    {
        writer.writeLong(position, valDual);
    }
}
