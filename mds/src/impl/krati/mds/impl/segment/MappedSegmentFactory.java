package krati.mds.impl.segment;

import java.io.File;
import java.io.IOException;

/**
 * MappedSegmentFactory
 * 
 * @author jwu
 *
 */
public class MappedSegmentFactory implements SegmentFactory
{
    @Override
    public Segment createSegment(int segmentId, File segmentFile, int initialSizeMB, Segment.Mode mode) throws IOException
    {
        return new MappedSegment(segmentId, segmentFile, initialSizeMB, mode);
    }
}
