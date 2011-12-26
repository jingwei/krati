/*
 * Copyright (c) 2010-2011 LinkedIn, Inc
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
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.apache.log4j.Logger;

/**
 * MemorySegment
 * 
 * @author jwu
 * 
 */
public class MemorySegment extends AbstractSegment {
    private final static Logger _log = Logger.getLogger(MemorySegment.class);
    private ByteBuffer _buffer;

    public MemorySegment(int segmentId, File segmentFile, int initialSizeMB, Segment.Mode mode) throws IOException {
        super(segmentId, segmentFile, initialSizeMB, mode);
    }

    @Override
    protected void init() throws IOException {
        _buffer = initByteBuffer();

        if (!getSegmentFile().exists()) {
            if (!getSegmentFile().createNewFile()) {
                String msg = "Failed to create " + getSegmentFile().getAbsolutePath();

                _log.error(msg);
                throw new IOException(msg);
            }

            RandomAccessFile raf = new RandomAccessFile(getSegmentFile(), "rw");
            raf.setLength(getInitialSize());
            raf.close();
        }

        if (getMode() == Segment.Mode.READ_ONLY) {
            _raf = new RandomAccessFile(getSegmentFile(), "r");

            checkSegmentSize();

            _channel = _raf.getChannel();
            _channel.position(0);

            // read into memory buffer
            _channel.read(_buffer);

            loadHeader();

            _log.info("Segment " + getSegmentId() + " loaded: " + getHeader());
        } else {
            _raf = new RandomAccessFile(getSegmentFile(), "rw");

            checkSegmentSize();

            _channel = _raf.getChannel();
            _channel.position(0);

            initHeader();

            _log.info("Segment " + getSegmentId() + " initialized: " + getStatus());
        }
    }

    protected ByteBuffer initByteBuffer() {
        int bufferLength = (int) ((_initSizeMB < Segment.maxSegmentFileSizeMB) ? _initSizeBytes : (_initSizeBytes - 1));
        return ByteBuffer.wrap(new byte[bufferLength]);
    }

    @Override
    public long getAppendPosition() throws IOException {
        return _buffer.position();
    }

    @Override
    public void setAppendPosition(long newPosition) throws IOException {
        _buffer.position((int) newPosition);
    }

    @Override
    public int appendInt(int value) throws IOException, SegmentOverflowException, SegmentReadOnlyException {
        if (isReadOnly()) {
            throw new SegmentReadOnlyException(this);
        }

        try {
            int pos = _buffer.position();
            _buffer.putInt(value);
            incrLoadSize(4);
            return pos;
        } catch (BufferOverflowException boe) {
            asReadOnly();
            throw new SegmentOverflowException(this);
        }
    }

    @Override
    public int appendLong(long value) throws IOException, SegmentOverflowException, SegmentReadOnlyException {
        if (isReadOnly()) {
            throw new SegmentReadOnlyException(this);
        }

        try {
            int pos = _buffer.position();
            _buffer.putLong(value);
            incrLoadSize(8);
            return pos;
        } catch (BufferOverflowException boe) {
            asReadOnly();
            throw new SegmentOverflowException(this);
        }
    }

    @Override
    public int appendShort(short value) throws IOException, SegmentOverflowException, SegmentReadOnlyException {
        if (isReadOnly()) {
            throw new SegmentReadOnlyException(this);
        }

        try {
            int pos = _buffer.position();
            _buffer.putShort(value);
            incrLoadSize(2);
            return pos;
        } catch (BufferOverflowException boe) {
            asReadOnly();
            throw new SegmentOverflowException(this);
        }
    }

    @Override
    public int append(byte[] data) throws IOException, SegmentOverflowException, SegmentReadOnlyException {
        if (isReadOnly()) {
            throw new SegmentReadOnlyException(this);
        }

        try {
            int pos = _buffer.position();
            _buffer.put(data);
            incrLoadSize(data.length);
            return pos;
        } catch (BufferOverflowException boe) {
            asReadOnly();
            throw new SegmentOverflowException(this);
        }
    }

    @Override
    public int append(byte[] data, int offset, int length) throws IOException, SegmentOverflowException, SegmentReadOnlyException {
        if (isReadOnly()) {
            throw new SegmentReadOnlyException(this);
        }

        try {
            int pos = _buffer.position();
            _buffer.put(data, offset, length);
            incrLoadSize(length);
            return pos;
        } catch (BufferOverflowException boe) {
            asReadOnly();
            throw new SegmentOverflowException(this);
        }
    }

    @Override
    public int readInt(int pos) throws IOException {
        return _buffer.getInt(pos);
    }

    @Override
    public long readLong(int pos) throws IOException {
        return _buffer.getLong(pos);
    }

    @Override
    public short readShort(int pos) throws IOException {
        return _buffer.getShort(pos);
    }

    @Override
    public void read(int pos, byte[] dst) throws IOException {
        System.arraycopy(_buffer.array(), pos, dst, 0, dst.length);
    }

    @Override
    public void read(int pos, byte[] dst, int offset, int length) throws IOException {
        System.arraycopy(_buffer.array(), pos, dst, offset, length);
    }

    public int transferTo(int pos, int length, Segment targetSegment) throws IOException {
        if ((pos + length) <= _buffer.position()) {
            targetSegment.append(_buffer.array(), pos, length);
            return length;
        }

        throw new SegmentOverflowException(this, SegmentOverflowException.Type.READ_OVERFLOW);
    }

    @Override
    public int transferTo(int pos, int length, WritableByteChannel targetChannel) throws IOException {
        /**
         * Channel-based JavaNio zero copy (channel-to-channel transfer) does
         * not work for MemorySegment because: 1. MemorySegment uses a large
         * amount of memory. 2. OS IO cache also uses a large amount of memory.
         * 3. Both will compete for memory and cause memory thrashing.
         * 
         * For example, a worst case scenario can be something like the
         * following:
         * 
         * The machine has 24 GB, and JVM has -Xmx16GB, and each write is about
         * 1 to 2 KB.
         * 
         * Each MemorySegment is almost 50% fragmented. For 10GB data, the total
         * memory hold by MemorySegment can be approximately ~20GB. When
         * compaction kicks off, the compactor will basically compact all the
         * segments one by one. Channel-based zero copy can extend IO cache up
         * to 10-20GB.
         * 
         * This can put extreme pressure on memory and decrease the write
         * performance of MemorySegment significantly. The test shows the write
         * rate of MemorySegment can decrease from 20~30/ms to 0.5/ms.
         * 
         * The better solution is to write data from memory to channel directly.
         */
        if ((pos + length) <= _buffer.position()) {
            targetChannel.write(ByteBuffer.wrap(_buffer.array(), (int) pos, length));
            return length;
        }

        throw new SegmentOverflowException(this, SegmentOverflowException.Type.READ_OVERFLOW);
    }

    @Override
    public synchronized void asReadOnly() throws IOException {
        if (getMode() == Segment.Mode.READ_WRITE) {
            force();
            _segMode = Segment.Mode.READ_ONLY;
            _log.info("Segment " + getSegmentId() + " switched to " + getMode());
        }
    }

    @Override
    public synchronized void force() throws IOException {
        if (_channel == null) return;
        if (getMode() == Segment.Mode.READ_WRITE) {
            int offset = (int) _channel.position();
            int length = _buffer.position() - offset;
            if (length > 0) {
                _channel.write(ByteBuffer.wrap(_buffer.array(), offset, length));
            }

            long currentTime = System.currentTimeMillis();
            ByteBuffer bb = ByteBuffer.wrap(new byte[8]);
            bb.putLong(currentTime);
            bb.flip();
            _channel.write(bb, 0);
            _lastForcedTime = currentTime;
        }

        _channel.force(true);
        _log.info("Segment " + getSegmentId() + " forced: " + getStatus());
    }

    @Override
    public synchronized void close(boolean force) throws IOException {
        if (force) {
            force();
        }
        
        if (_channel != null) {
            _channel.close();
            _channel = null;
        }
        
        if (_raf != null) {
            _raf.close();
            _raf = null;
        }
    }

    @Override
    public synchronized void reinit() throws IOException {
        _buffer.clear();
        _segMode = Segment.Mode.READ_WRITE;

        if (!getSegmentFile().exists()) {
            if (!getSegmentFile().createNewFile()) {
                String msg = "Failed to create " + getSegmentFile().getAbsolutePath();

                _log.error(msg);
                throw new IOException(msg);
            }

            RandomAccessFile raf = new RandomAccessFile(getSegmentFile(), "rw");
            raf.setLength(getInitialSize());
            raf.close();
        }

        _raf = new RandomAccessFile(getSegmentFile(), "rw");

        checkSegmentSize();

        _channel = _raf.getChannel();
        _channel.position(0);

        initHeader();

        _log.info("Segment " + getSegmentId() + " initialized: " + getStatus());
    }

    @Override
    public final boolean isRecyclable() {
        return true;
    }

    @Override
    public final boolean canReadFromBuffer() {
        return true;
    }

    @Override
    public final boolean canAppendToBuffer() {
        return true;
    }
}
