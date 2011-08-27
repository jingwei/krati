package krati.retention;

/**
 * InvalidPositionException
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/14, 2011 - Created
 */
public class InvalidPositionException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public InvalidPositionException(Position pos) {
        super("Invalid position: " + pos);
    }
    
    public InvalidPositionException(String message, Position pos) {
        super(message + ": " + pos);
    }
}
