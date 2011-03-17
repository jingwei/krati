package krati.core.array.entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * EntryValueBatch
 * 
 * @author jwu
 * 
 */
public abstract class EntryValueBatch {
    protected final int _valueSize;
    protected final int _valueCapacity;
    protected final ByteBuffer _buffer;
    
    protected EntryValueBatch(int valueSize, int valueCapacity) {
        this._valueSize = valueSize;
        this._valueCapacity = valueCapacity;
        this._buffer = ByteBuffer.allocate(_valueCapacity * _valueSize);
    }
    
    public int getCapacity() {
        return _valueCapacity;
    }
    
    public int getByteCapacity() {
        return _buffer.capacity();
    }
    
    public ByteBuffer getInternalBuffer() {
        return _buffer;
    }
    
    public void write(FileChannel channel) throws IOException {
        channel.write(ByteBuffer.wrap(_buffer.array(), 0, _buffer.position()));
    }
    
    public void clear() {
        _buffer.clear();
    }
}
