package krati.core.segment;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

/**
 * WriteBufferSegment
 * 
 * @author jwu
 * 
 */
public class WriteBufferSegment extends AbstractSegment {
    private final static Logger _log = Logger.getLogger(WriteBufferSegment.class);
    private Queue<ByteBuffer> _bufferQueue = null;
    private ByteBuffer _buffer = null;

    public WriteBufferSegment(int segmentId, File segmentFile, int initialSizeMB, Mode mode, ConcurrentLinkedQueue<ByteBuffer> bufferQueue) throws IOException {
        super(segmentId, segmentFile, initialSizeMB, mode);
        _bufferQueue = bufferQueue;
        if (mode == Mode.READ_WRITE) {
            _buffer = bufferQueue.poll();
            if (_buffer != null) {
                _log.info("ByteBuffer obtained");
            }
        }
        this.init();
    }

    @Override
    protected void init() throws IOException {
        if (_bufferQueue == null)
            return;

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

            if (_raf.length() != getInitialSize()) {
                int rafSizeMB = (int) (_raf.length() / 1024L / 1024L);
                throw new SegmentFileSizeException(getSegmentFile().getCanonicalPath(), rafSizeMB, getInitialSizeMB());
            }

            _channel = _raf.getChannel();
            _channel.position(0);

            loadHeader();

            _log.info("Segment " + getSegmentId() + " loaded: " + getHeader());
        } else {
            if (_buffer == null) {
                int bufferLength = (int) ((_initSizeMB < Segment.maxSegmentFileSizeMB) ? _initSizeBytes : (_initSizeBytes - 1));
                _buffer = ByteBuffer.wrap(new byte[bufferLength]);
                _log.info("ByteBuffer created: " + bufferLength);
            }
            _buffer.clear();

            _raf = new RandomAccessFile(getSegmentFile(), "rw");

            if (_raf.length() != getInitialSize()) {
                int rafSizeMB = (int) (_raf.length() / 1024L / 1024L);
                throw new SegmentFileSizeException(getSegmentFile().getCanonicalPath(), rafSizeMB, getInitialSizeMB());
            }

            _channel = _raf.getChannel();
            _channel.position(0);

            initHeader();

            _log.info("Segment " + getSegmentId() + " initialized: " + getStatus());
        }
    }

    @Override
    public long getAppendPosition() throws IOException {
        return isReadOnly() ? _channel.position() : _buffer.position();
    }

    @Override
    public void setAppendPosition(long newPosition) throws IOException {
        if (isReadOnly()) {
            _channel.position(newPosition);
        } else {
            _buffer.position((int) newPosition);
        }
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
        if (isReadOnly()) {
            ByteBuffer bb = ByteBuffer.wrap(new byte[4]);
            _channel.read(bb, pos);
            return bb.getInt(0);
        } else {
            return _buffer.getInt(pos);
        }
    }

    @Override
    public long readLong(int pos) throws IOException {
        if (isReadOnly()) {
            ByteBuffer bb = ByteBuffer.wrap(new byte[8]);
            _channel.read(bb, pos);
            return bb.getLong(0);
        } else {
            return _buffer.getLong(pos);
        }
    }

    @Override
    public short readShort(int pos) throws IOException {
        if (isReadOnly()) {
            ByteBuffer bb = ByteBuffer.wrap(new byte[2]);
            _channel.read(bb, pos);
            return bb.getShort(0);
        } else {
            return _buffer.getShort(pos);
        }
    }

    @Override
    public void read(int pos, byte[] dst) throws IOException {
        if (isReadOnly()) {
            ByteBuffer bb = ByteBuffer.wrap(dst);
            _channel.read(bb, pos);
        } else {
            System.arraycopy(_buffer.array(), pos, dst, 0, dst.length);
        }
    }

    @Override
    public void read(int pos, byte[] dst, int offset, int length) throws IOException {
        if (isReadOnly()) {
            ByteBuffer bb = ByteBuffer.wrap(dst, offset, length);
            _channel.read(bb, pos);
        } else {
            System.arraycopy(_buffer.array(), pos, dst, offset, length);
        }
    }

    @Override
    public int transferTo(int pos, int length, Segment targetSegment) throws IOException {
        if (isReadOnly()) {
            if ((pos + length) <= _initSizeBytes) {
                byte[] dst = new byte[length];
                this.read(pos, dst);

                targetSegment.append(dst);
                return length;
            }
        } else {
            if ((pos + length) <= _buffer.position()) {
                targetSegment.append(_buffer.array(), pos, length);
                return length;
            }
        }

        throw new SegmentOverflowException(this, SegmentOverflowException.Type.READ_OVERFLOW);
    }

    @Override
    public int transferTo(int pos, int length, WritableByteChannel targetChannel) throws IOException {
        if (isReadOnly()) {
            return (int) _channel.transferTo(pos, length, targetChannel);
        } else {
            if ((pos + length) <= _buffer.position()) {
                targetChannel.write(ByteBuffer.wrap(_buffer.array(), pos, length));
                return length;
            }

            throw new SegmentOverflowException(this, SegmentOverflowException.Type.READ_OVERFLOW);
        }
    }

    @Override
    public synchronized void asReadOnly() throws IOException {
        if (getMode() == Segment.Mode.READ_WRITE) {
            force();
            _segMode = Segment.Mode.READ_ONLY;

            if (_buffer != null) {
                _log.info("ByteBuffer returned");
                _bufferQueue.add(_buffer);
                _buffer = null;
            }

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
        if (force)
            force();

        if (_buffer != null) {
            _bufferQueue.add(_buffer);
            _buffer = null;
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
    public void reinit() throws IOException {
        throw new UnsupportedOperationException("reinit not supported");
    }

    @Override
    public boolean isRecyclable() {
        return false;
    }

    @Override
    public boolean canReadFromBuffer() {
        return false;
    }

    @Override
    public boolean canAppendToBuffer() {
        return true;
    }
}
