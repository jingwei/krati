package krati.core.segment;

import java.io.File;
import java.io.IOException;

/**
 * MemorySegmentFactory
 * 
 * @author jwu
 * 
 */
public class MemorySegmentFactory implements SegmentFactory {
    
    @Override
    public Segment createSegment(int segmentId, File segmentFile, int initialSizeMB, Segment.Mode mode) throws IOException {
        return new MemorySegment(segmentId, segmentFile, initialSizeMB, mode);
    }
}
