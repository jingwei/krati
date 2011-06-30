package krati.io;

/**
 * SerializationException
 * 
 * An exception is thrown by {#link krati.io.Serializer Serializer} if an object
 * cannot be serialized to a byte array or de-serialized from a byte array.
 * 
 * @author jwu
 * 06/29, 2011
 */
public class SerializationException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public SerializationException(String message) {
        super(message);
    }
    
    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
