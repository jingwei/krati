package krati.mds.impl.segment;

/**
 * SegmentReadOnlyException
 * 
 * @author jwu
 *
 */
public class SegmentReadOnlyException extends SegmentException
{
    private final static long serialVersionUID = 1L;
    private final Segment segment;
    
    public SegmentReadOnlyException(Segment seg)
    {
        super("Cannot write to read-only segment: " + seg.getSegmentId());
        this.segment = seg;
    }
    
    public Segment getSegment()
    {
        return segment;
    }

}
