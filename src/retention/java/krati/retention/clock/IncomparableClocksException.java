package krati.retention.clock;

/**
 * IncomparableClocksException
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/11, 2011 - Created <br/>
 */
public class IncomparableClocksException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public IncomparableClocksException(Clock c1, Clock c2) {
        super("Incomparable clocks: " + c1 + ", " + c2);
    }
}
