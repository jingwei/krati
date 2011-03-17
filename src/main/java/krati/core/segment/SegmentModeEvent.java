package krati.core.segment;

/**
 * SegmentModeEvent
 * 
 * @author jwu
 * 
 */
public class SegmentModeEvent {
    private final Segment _segment;

    public SegmentModeEvent(Segment segment) {
        this._segment = segment;
    }

    public final Segment getSegment() {
        return _segment;
    }

    public final Segment.Mode getSegmentMode() {
        return _segment.getMode();
    }
}
