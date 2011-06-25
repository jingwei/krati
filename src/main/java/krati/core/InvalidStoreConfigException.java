package krati.core;

/**
 * InvalidStoreConfigException
 * 
 * @author jwu
 * 06/25, 2011
 * 
 */
public class InvalidStoreConfigException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public InvalidStoreConfigException(String message) {
        super(message);
    }
}
