package krati.sos;

/**
 * ObjectConstructionException
 * 
 * An exception is thrown by <code>ObjectSerializer</code>
 * if an object cannot be constructed (i.e. de-serialized) from a given byte array.
 * 
 * @author jwu
 *
 */
public class ObjectConstructionException extends RuntimeException {
    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 1L;
    
    public ObjectConstructionException(String message) {
        super(message);
    }
    
    public ObjectConstructionException(String message, Throwable cause) {
        super(message, cause);
    }
}
