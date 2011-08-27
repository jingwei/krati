package krati.retention;

/**
 * SimpleEventBatchCursor
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 07/31, 2011 - Created
 */
public class SimpleEventBatchCursor implements EventBatchCursor {
    private final int _batchLookup;
    private final EventBatchHeader _batchHeader;
    
    /**
     * SimpleEventBatchCursor
     * 
     * @param batchLookup - the lookup index of an EventBatch
     * @param batchHeader - the batch header of an EventBatch
     */
    public SimpleEventBatchCursor(int batchLookup, EventBatchHeader batchHeader) {
        this._batchLookup = batchLookup;
        this._batchHeader = batchHeader;
    }
    
    @Override
    public EventBatchHeader getHeader() {
        return _batchHeader;
    }
    
    @Override
    public int getLookup() {
        return _batchLookup;
    }
}
