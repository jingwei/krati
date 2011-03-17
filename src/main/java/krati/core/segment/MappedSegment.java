package krati.core.segment;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.log4j.Logger;

/**
 * MappedSegment
 * 
 * @author jwu
 * 
 */
public class MappedSegment extends AbstractSegment {
    private final static Logger _log = Logger.getLogger(MappedSegment.class);
    private MappedByteBuffer _mmapBuffer;

    public MappedSegment(int segmentId, File segmentFile, int initialSizeMB, Segment.Mode mode) throws IOException {
        super(segmentId, segmentFile, initialSizeMB, mode);
    }

    @Override
    protected void init() throws IOException {
        long bufferLength = (_initSizeMB < Segment.maxSegmentFileSizeMB) ? _initSizeBytes : (_initSizeBytes - 1);

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
            // Load MappedByteBuffer
            _raf = new RandomAccessFile(getSegmentFile(), "r");

            if (_raf.length() != getInitialSize()) {
                int rafSizeMB = (int) (_raf.length() / 1024L / 1024L);
                throw new SegmentFileSizeException(getSegmentFile().getCanonicalPath(), rafSizeMB, getInitialSizeMB());
            }

            _channel = _raf.getChannel();
            _mmapBuffer = _channel.map(FileChannel.MapMode.READ_ONLY, 0, bufferLength);

            loadHeader();

            _log.info("Segment " + getSegmentId() + " loaded: " + getHeader());
        } else {
            // Create MappedByteBuffer
            _raf = new RandomAccessFile(getSegmentFile(), "rw");

            if (_raf.length() != getInitialSize()) {
                int rafSizeMB = (int) (_raf.length() / 1024L / 1024L);
                throw new SegmentFileSizeException(getSegmentFile().getCanonicalPath(), rafSizeMB, getInitialSizeMB());
            }

            _channel = _raf.getChannel();
            _mmapBuffer = _channel.map(FileChannel.MapMode.READ_WRITE, 0, bufferLength);

            initHeader();

            _log.info("Segment " + getSegmentId() + " initialized: " + getStatus());
        }
    }

    @Override
    public long getAppendPosition() {
        return _mmapBuffer.position();
    }

    @Override
    public void setAppendPosition(long newPosition) {
        _mmapBuffer.position((int) newPosition);
    }

    @Override
    public int appendInt(int value) throws IOException, SegmentOverflowException, SegmentReadOnlyException {
        if (isReadOnly()) {
            throw new SegmentReadOnlyException(this);
        }

        try {
            int pos = _mmapBuffer.position();
            _mmapBuffer.putInt(value);
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
            int pos = _mmapBuffer.position();
            _mmapBuffer.putLong(value);
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
            int pos = _mmapBuffer.position();
            _mmapBuffer.putShort(value);
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
            int pos = _mmapBuffer.position();
            _mmapBuffer.put(data, 0, data.length);
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
            int pos = _mmapBuffer.position();
            _mmapBuffer.put(data, offset, length);
            incrLoadSize(length);
            return pos;
        } catch (BufferOverflowException boe) {
            asReadOnly();
            throw new SegmentOverflowException(this);
        }
    }

    @Override
    public int readInt(int pos) {
        return _mmapBuffer.getInt(pos);
    }

    @Override
    public long readLong(int pos) {
        return _mmapBuffer.getLong(pos);
    }

    @Override
    public short readShort(int pos) {
        return _mmapBuffer.getShort(pos);
    }

    @Override
    public void read(int pos, byte[] dst) {
        for (int i = 0; i < dst.length; i++) {
            dst[i] = _mmapBuffer.get(pos + i);
        }
    }

    @Override
    public void read(int pos, byte[] dst, int offset, int length) {
        for (int i = 0; i < length; i++) {
            dst[offset + i] = _mmapBuffer.get(pos + i);
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
    public synchronized void force() {
        if (getMode() == Segment.Mode.READ_WRITE) {
            _lastForcedTime = System.currentTimeMillis();
            _mmapBuffer.putLong(0, _lastForcedTime);
        }

        _mmapBuffer.force();
        _log.info("Segment " + getSegmentId() + " forced: " + getStatus());
    }

    @Override
    public synchronized void close(boolean force) throws IOException {
        if (force) {
            force();
            _channel.force(true);
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
