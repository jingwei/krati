package krati.core.segment;

import java.io.File;
import java.io.IOException;

/**
 * SegmentFactory
 * 
 * @author jwu
 * 
 */
public interface SegmentFactory {
    
    public Segment createSegment(int segmentId, File segmentFile, int initialSizeMB, Segment.Mode mode) throws IOException;
}
