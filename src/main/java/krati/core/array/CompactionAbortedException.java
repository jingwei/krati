package krati.core.array;

/**
 * CompactionAbortedException
 * 
 * @author jwu
 * 
 */
public class CompactionAbortedException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public CompactionAbortedException() {
        super("Compaction aborted");
    }
}
