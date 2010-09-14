package krati.store;

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
    
    public static InvalidDataException invalidSizeException(int dataSize) {
        return new InvalidDataException("Invalid data size: " + dataSize);
    }
    
    public static InvalidDataException invalidSizeException(int dataSize, int sizeExpected) {
        return new InvalidDataException("Invalid data size: " + dataSize + ", " + sizeExpected + " expected");
    }
}
