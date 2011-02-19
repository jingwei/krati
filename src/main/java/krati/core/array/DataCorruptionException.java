package krati.core.array;

/**
 * DataCorruptionException.
 * 
 * @author jwu
 *
 */
public class DataCorruptionException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public DataCorruptionException(String message) {
        super(message);
    }
    
    public DataCorruptionException(int index) {
        super("Data corruption at index " + index);
    }
}
