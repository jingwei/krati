package krati.store;

/**
 * StoreClosedException
 * 
 * @author jwu
 * 06/04, 2011
 * 
 */
public class StoreClosedException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public StoreClosedException() {
        super("Store closed");
    }
    
    public StoreClosedException(String message) {
        super(message);
    }
    
    public StoreClosedException(String message, Throwable cause) {
        super(message, cause);
    }
}
