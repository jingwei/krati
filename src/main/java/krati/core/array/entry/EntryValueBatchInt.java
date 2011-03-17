package krati.core.array.entry;

/**
 * EntryValueBatchInt
 * 
 * @author jwu
 * 
 */
public class EntryValueBatchInt extends EntryValueBatch {
    
    public EntryValueBatchInt() {
        this(1000);
    }
    
    public EntryValueBatchInt(int capacity) {
        /*
         * EntryValueInt int position; int value; long scn;
         */
        super(16, capacity);
    }
    
    public void add(int pos, int val, long scn) {
        _buffer.putInt(pos);  /* array position */
        _buffer.putInt(val);  /* data value */
        _buffer.putLong(scn); /* SCN value */
    }
}
