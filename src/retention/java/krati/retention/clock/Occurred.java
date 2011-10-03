package krati.retention.clock;

/**
 * Occurred defines the result of comparing two Clocks <tt>c1</tt> and <tt>c2</tt>
 * 
 * <pre>
 *   c1 occurred EQUICONCURRENTLY to c2
 *   c1 occurred CONCURRENTLY to c2
 *   c1 occurred BEFORE c2
 *   c1 occurred AFTER c2
 * </pre>
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 09/27, 2011 - Created <br/>
 */
public enum Occurred {
    EQUICONCURRENTLY,
    CONCURRENTLY,
    BEFORE,
    AFTER;
}
