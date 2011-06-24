package krati.core;

/**
 * CompactionAbortedException
 * 
 * @author jwu
 * 
 */
public class CompactionAbortedException extends OperationAbortedException {
    private static final long serialVersionUID = 1L;
    
    public CompactionAbortedException() {
        super("Compaction aborted");
    }
    
    public CompactionAbortedException(Throwable cause) {
        super("Compaction aborted", cause);
    }
}
