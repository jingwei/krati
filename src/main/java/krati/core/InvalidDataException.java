package krati.core;

/**
 * InvalidDataException.
 * 
 * @author jwu
 * 
 */
public class InvalidDataException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public InvalidDataException(String message) {
        super(message);
    }
    
    public InvalidDataException(int index) {
        super("Invalid data at index " + index);
    }
}
