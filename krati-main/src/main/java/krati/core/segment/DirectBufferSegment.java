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
import java.nio.channels.WritableByteChannel;

import org.apache.log4j.Logger;

/**
 * DirectBufferSegment uses system memory instead of JVM memory. Using DirectBufferSegment requires
 * the proper setup using JVM parameter <code>-XX:MaxDirectMemorySize</code>
 * 
 * @author jwu
 * @since 08/20, 2012
 */
public class DirectBufferSegment extends MemorySegment {
    /**
     * The logger
     */
    private static Logger _log = Logger.getLogger(DirectBufferSegment.class);
    
    /**
     * Create a new DirectBufferSegment.
     * 
     * @param segmentId     - the segmentId
     * @param segmentFile   - the segmentFile
     * @param initialSizeMB - the segment initial size in MB
     * @param mode          - the segment mode
     * @throws IOException if this DirectBufferSegment cannot be created.
     */
    public DirectBufferSegment(int segmentId, File segmentFile, int initialSizeMB, Mode mode) throws IOException {
        super(segmentId, segmentFile, initialSizeMB, mode);
    }
    
    @Override
    protected Logger logger() {
        return _log;
    }

    @Override
    protected ByteBuffer initByteBuffer() {
        int bufferLength = (int) ((_initSizeMB < Segment.maxSegmentFileSizeMB) ? _initSizeBytes : (_initSizeBytes - 1));
        return ByteBuffer.allocateDirect(bufferLength);
    }
    
    @Override
    public void read(int pos, byte[] dst) throws IOException {
        for (int i = 0; i < dst.length; i++) {
            dst[i] = _buffer.get(pos + i);
        }
    }
    
    @Override
    public void read(int pos, byte[] dst, int offset, int length) {
        for (int i = 0; i < length; i++) {
            dst[offset + i] = _buffer.get(pos + i);
        }
    }
    
    @Override
    public int transferTo(int pos, int length, Segment targetSegment) throws IOException {
        if ((pos + length) <= _initSizeBytes) {
            byte[] dst = new byte[length];
            this.read(pos, dst);

            targetSegment.append(dst);
            return length;
        }
        
        throw new SegmentOverflowException(this, SegmentOverflowException.Type.READ_OVERFLOW);
    }
    
    @Override
    public int transferTo(int pos, int length, WritableByteChannel targetChannel) throws IOException {
        if ((pos + length) <= _buffer.position()) {
            byte[] dst = new byte[length];
            this.read(pos, dst);
            targetChannel.write(ByteBuffer.wrap(dst));
            return length;
        }
        
        throw new SegmentOverflowException(this, SegmentOverflowException.Type.READ_OVERFLOW);
    }
    
    @Override
    public synchronized void force() throws IOException {
        if (_channel == null) return;
        if (getMode() == Segment.Mode.READ_WRITE) {
            int offset = (int) _channel.position();
            int length = _buffer.position() - offset;
            if (length > 0) {
                byte[] dst = new byte[length];
                read(offset, dst);
                _channel.write(ByteBuffer.wrap(dst));
            }
            
            long currentTime = System.currentTimeMillis();
            ByteBuffer bb = ByteBuffer.wrap(new byte[8]);
            bb.putLong(currentTime);
            bb.flip();
            _channel.write(bb, 0);
            _lastForcedTime = currentTime;
        }

        _channel.force(true);
        logger().info("Segment " + getSegmentId() + " forced: " + getStatus());
    }
}
