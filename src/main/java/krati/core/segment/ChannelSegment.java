package krati.core.segment;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.BufferOverflowException;
import java.nio.channels.WritableByteChannel;

import org.apache.log4j.Logger;

/**
 * ChannelSegment
 * 
 * @author jwu
 * 
 */
public class ChannelSegment extends AbstractSegment {
    private final static Logger _log = Logger.getLogger(ChannelSegment.class);

    public ChannelSegment(int segmentId, File segmentFile, int initialSizeMB, Segment.Mode mode) throws IOException {
        super(segmentId, segmentFile, initialSizeMB, mode);
    }

    @Override
    protected void init() throws IOException {
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
        return _channel.position();
    }

    @Override
    public void setAppendPosition(long newPosition) throws IOException {
        _channel.position(newPosition);
    }

    @Override
    public int appendInt(int value) throws IOException, SegmentOverflowException, SegmentReadOnlyException {
        if (isReadOnly()) {
            throw new SegmentReadOnlyException(this);
        }

        try {
            int pos = (int) _channel.position();
            if ((pos + 4) >= _initSizeBytes) {
                throw new BufferOverflowException();
            }

            ByteBuffer bb = ByteBuffer.wrap(new byte[4]);
            bb.putInt(value);
            bb.flip();
            _channel.write(bb);
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
            int pos = (int) _channel.position();
            if ((pos + 8) >= _initSizeBytes) {
                throw new BufferOverflowException();
            }

            ByteBuffer bb = ByteBuffer.wrap(new byte[8]);
            bb.putLong(value);
            bb.flip();
            _channel.write(bb);
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
            int pos = (int) _channel.position();
            if ((pos + 2) >= _initSizeBytes) {
                throw new BufferOverflowException();
            }

            ByteBuffer bb = ByteBuffer.wrap(new byte[2]);
            bb.putShort(value);
            bb.flip();
            _channel.write(bb);
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
            int pos = (int) _channel.position();
            if ((pos + data.length) >= _initSizeBytes) {
                throw new BufferOverflowException();
            }

            ByteBuffer bb = ByteBuffer.wrap(data);
            _channel.write(bb);
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
            int pos = (int) _channel.position();
            if ((pos + length) >= _initSizeBytes) {
                throw new BufferOverflowException();
            }

            ByteBuffer bb = ByteBuffer.wrap(data, offset, length);
            _channel.write(bb);
            incrLoadSize(length);
            return pos;
        } catch (BufferOverflowException boe) {
            asReadOnly();
            throw new SegmentOverflowException(this);
        }
    }

    @Override
    public int readInt(int pos) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(new byte[4]);
        _channel.read(bb, pos);
        return bb.getInt(0);
    }

    @Override
    public long readLong(int pos) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(new byte[8]);
        _channel.read(bb, pos);
        return bb.getLong(0);
    }

    @Override
    public short readShort(int pos) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(new byte[2]);
        _channel.read(bb, pos);
        return bb.getShort(0);
    }

    @Override
    public void read(int pos, byte[] dst) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(dst);
        _channel.read(bb, pos);
    }

    @Override
    public void read(int pos, byte[] dst, int offset, int length) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(dst, offset, length);
        _channel.read(bb, pos);
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
        return (int) _channel.transferTo(pos, length, targetChannel);
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
        if (getMode() == Segment.Mode.READ_WRITE) {
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
    public void reinit() throws IOException, UnsupportedOperationException {
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
        return false;
    }
}
