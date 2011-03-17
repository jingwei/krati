package krati.core.array.entry;

/**
 * EntryValueBatchLong
 * 
 * @author jwu
 * 
 */
public class EntryValueBatchLong extends EntryValueBatch {
    
    public EntryValueBatchLong() {
        this(1000);
    }
    
    public EntryValueBatchLong(int capacity) {
        /*
         * EntryValueLong int position; long value; long scn;
         */
        super(20, capacity);
    }
    
    public void add(int pos, long val, long scn) {
        _buffer.putInt(pos);  /* array position */
        _buffer.putLong(val); /* data value */
        _buffer.putLong(scn); /* SCN value */
    }
}
