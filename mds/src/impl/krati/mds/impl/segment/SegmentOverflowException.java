package krati.mds.impl.segment;

/**
 * SegmentOverflowException
 * 
 * @author jwu
 *
 */
public class SegmentOverflowException extends SegmentException
{
    private final static long serialVersionUID = 1L;
    private final Segment segment;
    
    public SegmentOverflowException(Segment seg)
    {
        super("Overflow at segment: " + seg.getSegmentId());
        this.segment = seg;
    }
    
    public Segment getSegment()
    {
        return segment;
    }
}
