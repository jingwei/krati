package krati.core.segment;

/**
 * SegmentException
 * 
 * @author jwu
 * 
 */
public class SegmentException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public SegmentException(String message) {
        super(message);
    }
    
    public final static SegmentException segmentNotFound(int segId) {
        return new SegmentException("Segment not found: " + segId);
    }
}
