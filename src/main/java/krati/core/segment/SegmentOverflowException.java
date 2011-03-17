package krati.core.segment;

/**
 * SegmentOverflowException
 * 
 * @author jwu
 * 
 */
public class SegmentOverflowException extends SegmentException {
    private final static long serialVersionUID = 1L;
    private final Segment _segment;
    private final Type _overflowType;

    public SegmentOverflowException(Segment seg) {
        super("Overflow at segment: " + seg.getSegmentId());
        this._segment = seg;
        this._overflowType = Type.WRITE_OVERFLOW;
    }

    public SegmentOverflowException(Segment seg, Type type) {
        super(type + " at segment: " + seg.getSegmentId());
        this._segment = seg;
        this._overflowType = type;
    }

    public Segment getSegment() {
        return _segment;
    }

    public Type getOverflowType() {
        return _overflowType;
    }

    public static enum Type {
        READ_OVERFLOW {
            public String toString() {
                return "Read overflow";
            }
        },
        WRITE_OVERFLOW {
            public String toString() {
                return "Write overflow";
            }
        };
    }
}
