package krati.cds.impl.array.entry;

public class EntryValueBatchShort extends EntryValueBatch
{
    public EntryValueBatchShort()
    {
        this(1000);
    }
    
    public EntryValueBatchShort(int capacity)
    {
        /* EntryValueLong
         *   int position;
         *   short value;
         *   long scn;
         */
        super(14, capacity);
    }
    
    public void add(int pos, short val, long scn)
    {
        _buffer.putInt(pos);   /* array position */
        _buffer.putShort(val); /* data value     */
        _buffer.putLong(scn);  /* SCN value      */
    }
}
