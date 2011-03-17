package krati.core.segment;

import java.io.File;
import java.io.IOException;

/**
 * ChannelSegmentFactory
 * 
 * @author jwu
 * 
 */
public class ChannelSegmentFactory implements SegmentFactory {
    
    @Override
    public Segment createSegment(int segmentId, File segmentFile, int initialSizeMB, Segment.Mode mode) throws IOException {
        return new ChannelSegment(segmentId, segmentFile, initialSizeMB, mode);
    }
}
