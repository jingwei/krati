package krati.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.DataOutput;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.ByteBuffer;

/**
 * FastDataWriter: a simple writable channel, which is NOT thread safe.
 * 
 * Partially taken from org.xeril.util.io.FastDataWriteChannel.
 * 
 * @author jwu
 * 
 */
public class FastDataWriter implements DataWriter {
    public  final static int DEFAULT_BUFFER_SIZE = 8 * 1024;
    private final static int MAX_WRITE_ITERATIONS = 10;
    
    private File _file;
    private final ByteBuffer _buffer;
    private WritableByteChannel _channel;
    
    /**
     * Constructor
     */
    public FastDataWriter(File file) {
        this(file, DEFAULT_BUFFER_SIZE);
    }
    
    /**
     * Constructor
     */
    public FastDataWriter(File file, int bufferSize) {
        if(bufferSize < 512) {
            throw new IllegalArgumentException("bufferSize is too small: at least 512");
        }
        
        _file = file;
        _buffer = ByteBuffer.allocate(bufferSize);
    }
    
    /**
     * @param b the byte to be written.
     * @throws IOException if an I/O error occurs.
     * @see DataOutput#write(int)
     */
    public void write(int b) throws IOException {
        ensureAvailability(1);
        _buffer.put((byte) b);
    }
    
    /**
     * @param b the data.
     * @throws IOException if an I/O error occurs.
     * @see DataOutput#write(byte[])
     */
    public void write(byte b[]) throws IOException {
        write(b, 0, b.length);
    }
    
    /**
     * @param b   the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     * @throws IOException if an I/O error occurs.
     * @see DataOutput#write(byte[], int, int)
     */
    public void write(byte b[], int off, int len) throws IOException {
        int remaining = _buffer.remaining();

        // if enough space remaining => we can simply copy it to the buffer
        if(remaining >= len) {
            _buffer.put(b, off, len);
        } else {
            // not enough space left.. we squeeze as much as we can
            if(remaining > 0) {
                _buffer.put(b, off, remaining);
                off += remaining;
                len -= remaining;
            }
            
            // we then flush the buffer completely
            writeFullBuffer();

            // if what we need to write fits in the buffer then we add it to the buffer
            if(len < _buffer.capacity()) {
                _buffer.put(b, off, len);
            } else {
                // no need to buffer => we stream it directly
                writeFullBuffer(ByteBuffer.wrap(b, off, len));
            }
        }
    }
    
    /**
     * @param v the byte value to be written.
     * @throws IOException if an I/O error occurs.
     * @see DataOutput#writeByte(int)
     */
    public void writeByte(int v) throws IOException {
        ensureAvailability(1);
        _buffer.put((byte) v);
    }
    
    /**
     * @param v the <code>int</code> value to be written.
     * @throws IOException if an I/O error occurs.
     * @see DataOutput#writeInt(int)
     */
    public void writeInt(int v) throws IOException {
        ensureAvailability(4);
        _buffer.putInt(v);
    }
    
    /**
     * @param v the <code>short</code> value to be written.
     * @throws IOException if an I/O error occurs.
     * @see DataOutput#writeShort(int)
     */
    public void writeShort(short v) throws IOException {
        ensureAvailability(2);
        _buffer.putShort(v);
    }
    
    /**
     * @param v the <code>long</code> value to be written.
     * @throws IOException if an I/O error occurs.
     * @see DataOutput#writeLong(long)
     */
    public void writeLong(long v) throws IOException {
        ensureAvailability(8);
        _buffer.putLong(v);
    }
    
    /**
     * @param v the <code>float</code> value to be written.
     * @throws IOException if an I/O error occurs.
     * @see DataOutput#writeFloat(float)
     */
    public void writeFloat(float v) throws IOException {
        ensureAvailability(4);
        _buffer.putFloat(v);
    }
    
    /**
     * @param v the <code>double</code> value to be written.
     * @throws IOException if an I/O error occurs.
     * @see DataOutput#writeDouble(double)
     */
    public void writeDouble(double v) throws IOException {
        ensureAvailability(8);
        _buffer.putDouble(v);
    }
    
    /**
     * @param v the boolean to be written.
     * @throws IOException if an I/O error occurs.
     * @see DataOutput#writeBoolean(boolean)
     */
    public void writeBoolean(boolean v) throws IOException {
        ensureAvailability(1);
        _buffer.put((byte) (v ? 1 : 0));
    }
    
    @Override
    public File getFile() {
        return _file;
    }
    
    @Override
    public void open() throws IOException {
        FileOutputStream out = new FileOutputStream(_file);
        _channel = Channels.newChannel(out);
    }
    
    @Override
    public void close() throws IOException {
        flush();
        _channel.close();
    }
    
    @Override
    public void flush() throws IOException {
        writeFullBuffer();
    }
    
    @Override
    public void force() throws IOException {
        writeFullBuffer();
    }
    
    /**
     * Writes the full content of the buffer to the channel. If this method exits normally
     * (with no exception), then the buffer is entirely empty, ready to have more stuff written into it.
     *
     * @throws IOException
     */
    private void writeFullBuffer() throws IOException {
        _buffer.flip();
        writeFullBuffer(_buffer);
        _buffer.clear();
    }
    
    /**
     * Writes the full content of the buffer to the channel. If this method exits normally
     * (with no exception), then the buffer is entirely empty, ready to have more stuff written into it.
     *
     * @throws IOException
     */
    private void writeFullBuffer(ByteBuffer buffer) throws IOException {
        int iterationsCount = MAX_WRITE_ITERATIONS;
        int writeCount;
        
        while(buffer.remaining() > 0 && iterationsCount > 0) {
            writeCount = _channel.write(buffer);
            if(writeCount == 0) {
                // we decrement iterations count only when we could not write anything
                --iterationsCount;
            } else {
                // as long as we can write, we reset iterationsCount
                iterationsCount = MAX_WRITE_ITERATIONS;
            }
        }
        
        if(buffer.remaining() > 0)
            throw new IOException("couldn't write the buffer after " + MAX_WRITE_ITERATIONS + " tries");
    }
    
    private void ensureAvailability(int size) throws IOException {
        if(_buffer.remaining() < size)
            writeFullBuffer();
    }
    
    final static UnsupportedOperationException _unsupportedOpEx =
        new UnsupportedOperationException();
    
    @Override
    public void writeInt(long position, int value) throws IOException {
        throw _unsupportedOpEx;
    }
    
    @Override
    public void writeLong(long position, long value) throws IOException {
        throw _unsupportedOpEx;
    }

    @Override
    public void writeShort(long position, short value) throws IOException {
        throw _unsupportedOpEx;
    }

    @Override
    public long position() throws IOException {
        throw _unsupportedOpEx;
    }

    @Override
    public void position(long newPosition) throws IOException {
        throw _unsupportedOpEx;
    }
}
