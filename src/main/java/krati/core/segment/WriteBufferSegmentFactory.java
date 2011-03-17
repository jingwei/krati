package krati.core.segment;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * WriteBufferSegmentFactory
 * 
 * @author jwu
 * 
 */
public class WriteBufferSegmentFactory implements SegmentFactory {
    private final ConcurrentLinkedQueue<ByteBuffer> _bufferQueue = new ConcurrentLinkedQueue<ByteBuffer>();
    
    public WriteBufferSegmentFactory(int segmentFileSizeMB) {
        int bufferLength = (int) (segmentFileSizeMB * 1024L * 1024L);
        for (int i = 0; i < 3; i++) {
            ByteBuffer buffer = ByteBuffer.wrap(new byte[bufferLength]);
            _bufferQueue.add(buffer);
        }
    }
    
    @Override
    public Segment createSegment(int segmentId, File segmentFile, int initialSizeMB, Segment.Mode mode) throws IOException {
        return new WriteBufferSegment(segmentId, segmentFile, initialSizeMB, mode, _bufferQueue);
    }
}
