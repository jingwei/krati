package krati.core;

/**
 * OperationAbortedException
 * 
 * @author jwu
 * 06/12, 2011
 * 
 */
public class OperationAbortedException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public OperationAbortedException() {
        super("Operation aborted");
    }
    
    public OperationAbortedException(String message) {
        super(message);
    }
    
    public OperationAbortedException(Throwable cause) {
        super("Operation aborted", cause);
    }
    
    public OperationAbortedException(String message, Throwable cause) {
        super(message, cause);
    }
}
