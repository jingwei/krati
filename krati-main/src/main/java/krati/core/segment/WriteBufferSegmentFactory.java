/*
 * Copyright (c) 2010-2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
 * <p>
 * 04/24, 2012 - The default constructor made lazy to avoid read corruption<br/>
 */
public class WriteBufferSegmentFactory implements SegmentFactory {
    private volatile int lazyCount = 0;
    private final ConcurrentLinkedQueue<ByteBuffer> _bufferQueue = new ConcurrentLinkedQueue<ByteBuffer>();
    
    /**
     * Constructs a new instance of WriteBufferSegmentFactory.
     * 
     * <p>
     * This constructor is lazy and it does not allocates {@link ByteBuffer}
     * until this segment factory is called to create new segments.
     * </p>
     */
    public WriteBufferSegmentFactory() {}
    
    /**
     * Constructs a new instance of WriteBufferSegmentFactory.
     * 
     * <p>
     * This is not a lazy constructor. It directly allocates three {@link ByteBuffer}(s)
     * based on the specified <code>segmentFileSizeMB</code>.
     * </p>
     * 
     * @param segmentFileSizeMB - the segment file size in MB
     */
    public WriteBufferSegmentFactory(int segmentFileSizeMB) {
        if(segmentFileSizeMB < Segment.minSegmentFileSizeMB ||
           segmentFileSizeMB > Segment.maxSegmentFileSizeMB) {
            throw new IllegalArgumentException("Invalid argument: " + segmentFileSizeMB);
        }
        initializeBufferQueue(segmentFileSizeMB);
    }
    
    @Override
    public Segment createSegment(int segmentId, File segmentFile, int initialSizeMB, Segment.Mode mode) throws IOException {
        // No need to synchronize since there is only one thread calling this method.
        if(lazyCount == 0) {
            initializeBufferQueue(initialSizeMB);
        }
        
        return new WriteBufferSegment(segmentId, segmentFile, initialSizeMB, mode, _bufferQueue);
    }
    
    /**
     * Initialize the buffer queue with three ByteBuffer(s).
     * 
     * @param segmentFileSizeMB - the segment file size in MB
     */
    private void initializeBufferQueue(int segmentFileSizeMB) {
        int bufferLength = 
            (segmentFileSizeMB < Segment.maxSegmentFileSizeMB) ?
            (segmentFileSizeMB * 1024 * 1024) : Integer.MAX_VALUE;
            
        lazyCount = 3;
        for (int i = 0; i < lazyCount; i++) {
            ByteBuffer buffer = ByteBuffer.wrap(new byte[bufferLength]);
            _bufferQueue.add(buffer);
        }
    }
}
