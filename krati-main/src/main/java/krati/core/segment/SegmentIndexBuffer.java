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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * SegmentIndexBuffer
 * 
 * @author jwu
 * @since 08/25, 2012
 */
public class SegmentIndexBuffer implements Cloneable {
    /**
     * The segment ID.
     */
    protected int _segId;
    
    /**
     * The segment last forced time.
     */
    protected long _lastForcedTime;
    
    /**
     * The backing buffer.
     */
    protected ByteBuffer _buffer;
    
    /**
     * Whether this segment index buffer is dirty.
     */
    private volatile boolean _dirty = false; 
    
    /**
     * The storage header length (number of bytes).
     */
    public final static int HEADER_LENGTH = 16;
    
    /**
     * The storage footer length (number of bytes)
     */
    public final static int FOOTER_LENGTH = 16;
    
    /**
     * The MD5 length (number of bytes)
     */
    final static int MD5_LENGTH = FOOTER_LENGTH;
    
    /**
     * The total length of header and footer.
     */
    final static int HEADER_FOOTER_LENGTH = HEADER_LENGTH + FOOTER_LENGTH;
    
    /**
     * The default capacity (1024).
     */
    final static int DEFAULT_CAPACITY = 1024;
    
    /**
     * Creates a new SegmentIndexBuffer.  
     */
    public SegmentIndexBuffer() {
        _buffer = ByteBuffer.allocate(DEFAULT_CAPACITY << 3);
    }
    
    /**
     * Creates a new SegmentIndexBuffer.  
     * 
     * @param initialCapacity - the initial capacity
     */
    public SegmentIndexBuffer(int initialCapacity) {
        if(initialCapacity < 1) {
            initialCapacity = DEFAULT_CAPACITY;
        }
        _buffer = ByteBuffer.allocate(initialCapacity << 3);
    }
    
    /**
     * Gets the segment ID.
     */
    public int getSegmentId() {
        return _segId;
    }
    
    /**
     * Sets the segment ID.
     */
    public void setSegmentId(int segId) {
        this._segId = segId;
    }
    
    /**
     * Gets the segment last forced time.
     */
    public long getSegmentLastForcedTime() {
        return _lastForcedTime;
    }
    
    /**
     * Sets the segment last forced time.
     */
    public void setSegmentLastForcedTime(long lastForcedTime) {
        this._lastForcedTime = lastForcedTime;
    }
    
    /**
     * @return the number of {@link IndexOffset} pairs added to this SegmentIndexBuffer.
     */
    public int size() {
        return _buffer.position() >> 3;
    }
    
    /**
     * @return the number of {@link IndexOffset} pairs that can be added to this SegmentIndexBuffer.
     */
    public int capacity() {
        return _buffer.capacity() >> 3;
    }
    
    /**
     * Adds a pair of index and offset to this SegmentIndexBuffer.
     * 
     * @param index  - the array index
     * @param offset - the segment offset
     */
    public void add(int index, int offset) {
        ensureCapacity();
        _buffer.putInt(index);
        _buffer.putInt(offset);
    }
    
    /**
     * Adds a pair of index and offset to this SegmentIndexBuffer.
     * 
     * @param reuse - the reuse IndexOffset.
     */
    public void add(IndexOffset reuse) {
        ensureCapacity();
        _buffer.putInt(reuse.getIndex());
        _buffer.putInt(reuse.getOffset());
    }
    
    /**
     * Gets the pair of index and offset at the specified <code>pos</code>
     * 
     * @param pos - the position to SegmentIndexBuffer
     * @throws IndexOutOfBoundsException if the specified <code>pos</code> is no smaller than the known size.
     */
    public IndexOffset get(int pos) {
        int i = pos << 3;
        int index = _buffer.getInt(i);
        int offset = _buffer.getInt(i+4);
        return new IndexOffset(index, offset);
    }
    
    /**
     * Gets the pair of index and offset at the specified <code>pos</code>
     * 
     * @param pos - the position to SegmentIndexBuffer
     * @param reuse - the reuse IndexOffset to fill in
     * @throws IndexOutOfBoundsException if the specified <code>pos</code> is no smaller than the known size.
     */
    public void get(int pos, IndexOffset reuse) {
        int i = pos << 3;
        int index = _buffer.getInt(i);
        int offset = _buffer.getInt(i+4);
        reuse.reinit(index, offset);
    }
    
    /**
     * Reads from the specified <code>channel</code> into this SegmentIndexBuffer.
     * 
     * @param channel - the readable channel
     * @return the number of bytes read from the specified channel.
     * @throws IOException
     */
    public int read(ReadableByteChannel channel) throws IOException {
        // Header: segId (4), lastForcedTime (8), size (4)
        ByteBuffer header = ByteBuffer.allocate(HEADER_LENGTH);
        read(channel, header, HEADER_LENGTH, "Invalid Header");
        
        int segmentId = header.getInt(0);
        long lastForcedTime = header.getLong(4);
        int size = header.getInt(12);
        
        // Data
        int dataLength = size << 3;
        ByteBuffer data = ByteBuffer.allocate(dataLength);
        read(channel, data, dataLength, "Invalid Data");
        
        // Footer - MD5Digest (16)
        ByteBuffer md5 = ByteBuffer.allocate(MD5_LENGTH);
        read(channel, md5, MD5_LENGTH, "Invalid MD5");
        
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.reset();
            m.update(header.array());
            m.update(data.array());
            byte[] digest = ensure128BitMD5(m.digest());
            
            if(Arrays.equals(md5.array(), digest)) {
                setSegmentId(segmentId);
                setSegmentLastForcedTime(lastForcedTime);
                data.flip();
                put(data);
            }
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
        
        return (dataLength + HEADER_FOOTER_LENGTH);
    }
    
    /**
     * Writes this SegmentIndexBuffer to the specified <code>channel</code>.
     * 
     * @param channel - the writable channel
     * @return the number of bytes written to the specified channel.
     * @throws IOException
     */
    public int write(WritableByteChannel channel) throws IOException {
        // header
        ByteBuffer header = ByteBuffer.allocate(HEADER_LENGTH);
        header.putInt(getSegmentId());
        header.putLong(getSegmentLastForcedTime());
        header.putInt(size());
        
        // footer
        ByteBuffer footer;
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.reset();
            m.update(header.array());
            m.update(_buffer.array(), 0, _buffer.position());
            byte[] digest = ensure128BitMD5(m.digest());
            
            footer = ByteBuffer.allocate(MD5_LENGTH);
            footer.put(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
        
        // Write header, data, footer
        header.flip();
        channel.write(header);
        
        channel.write(ByteBuffer.wrap(_buffer.array(), 0, _buffer.position()));
        
        footer.flip();
        channel.write(footer);
        
        return (_buffer.position() + HEADER_FOOTER_LENGTH);
    }
    
    /**
     * Reads from the specified <code>channel</code>
     * 
     * @param channel  - the readable channel
     * @param bb       - the byte buffer to write
     * @param expected - the number of bytes expected to read
     * @param message  - the IOException message 
     * @throws IOException
     */
    protected void read(ReadableByteChannel channel, ByteBuffer bb, int expected, String message) throws IOException {
        int len = channel.read(bb);
        if (len != expected) {
            throw new IOException(message);
        }
    }
    
    /**
     * Ensure that the specified MD5 digest has 128 bits.
     */
    protected byte[] ensure128BitMD5(byte[] digest) {
        if(digest.length == MD5_LENGTH) {
            return digest;
        } else if(digest.length > MD5_LENGTH) {
            byte[] b = new byte[MD5_LENGTH];
            System.arraycopy(digest, 0, b, 0, MD5_LENGTH);
            return b;
        } else {
            byte[] b = new byte[MD5_LENGTH];
            System.arraycopy(digest, 0, b, 0, digest.length);
            Arrays.fill(b, digest.length, b.length, (byte)0);
            return b;
        }
    }
    
    /**
     * Ensure that the internal backing buffer has enough capacity for a new pair of index and offset.
     */
    protected void ensureCapacity() {
        if(_buffer.remaining() < 8) {
            ByteBuffer b = ByteBuffer.allocate(_buffer.capacity() << 1);
            _buffer.flip();
            b.put(_buffer);
            _buffer = b;
        }
    }
    
    /**
     * Bulk-put via {@link ByteBuffer}.
     */
    protected void put(ByteBuffer bb) {
       if(_buffer.remaining() < bb.remaining()) {
           int newCapacity = Math.max(_buffer.capacity() << 1, _buffer.position() + bb.remaining());
           ByteBuffer b = ByteBuffer.allocate(newCapacity);
           _buffer.flip();
           b.put(_buffer);
           _buffer = b;
       }
       
       _buffer.put(bb);
    }
    
    /**
     * Gets the backing {@link ByteBuffer}.
     */
    protected ByteBuffer getByteBuffer() {
        return _buffer;
    }
    
    /**
     * Clears this SegmentIndexBuffer.
     */
    public void clear() {
        _buffer.clear();
    }
    
    /**
     * Makes a clone of this SegmentIndexBuffer.
     */
    @Override
    public SegmentIndexBuffer clone() {
        SegmentIndexBuffer sib = new SegmentIndexBuffer(size());
        
        sib._buffer.put(_buffer.array(), 0, _buffer.position());
        sib.setSegmentId(_segId);
        sib.setSegmentLastForcedTime(_lastForcedTime);
        sib.setDirty(_dirty);
        
        return sib;
    }
    
    /**
     * Tests if this SegmentIndexBuffer is dirty.
     */
    public boolean isDirty() {
        return _dirty;
    }
    
    /**
     * Marks if this SegmentIndexBuffer is dirty.
     */
    public void setDirty(boolean b) {
        this._dirty = b;
    }
    
    /**
     * Marks this SegmentIndexBuffer as clean.
     */
    public void markAsClean() {
        this._dirty = false;
    }
    
    /**
     * Marks this SegmentIndexBuffer as dirty.
     */
    public void markAsDirty() {
        this._dirty = true;
    }
    
    /**
     * IndexOffset
     * 
     * @author jwu
     * @since 08/25, 2012
     */
    public static class IndexOffset {
        private int _index;
        private int _offset;
        
        /**
         * Creates a new IndexOffset.
         */
        public IndexOffset() {}
        
        /**
         * Creates a new IndexOffset.
         * 
         * @param index - the array index
         * @param offset - the segment offset
         */
        public IndexOffset(int index, int offset) {
            this._index = index;
            this._offset = offset;
        }
        
        /**
         * Reinitialize this IndexOffset.
         * 
         * @param index - the array index
         * @param offset - the segment offset
         */
        public void reinit(int index, int offset) {
            this._index = index;
            this._offset = offset;
        }
        
        public void setIndex(int index) {
            this._index = index;
        }
        
        public int getIndex() {
            return _index;
        }
        
        public void setOffset(int offset) {
            this._offset = offset;
        }
        
        public int getOffset() {
            return _offset;
        }
    }
}
