package krati.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * MappedReader
 * 
 * @author jwu
 * 
 */
public class MappedReader implements DataReader {
    private final File _file;
    private RandomAccessFile _raf;
    private MappedByteBuffer _mmapBuffer;
    
    /**
     * Create a file reader based on MappedByteBuffer.
     * 
     * The mapped region starts at position 0 (i.e. the first byte) and has a size up to the maximum integer.
     *   
     * @param file - file to read from.
     */
    public MappedReader(File file) {
        this._file = file;
    }
    
    @Override
    public File getFile() {
        return _file;
    }
    
    @Override
    public void open() throws IOException {
        if(!_file.exists()) {
            throw new IOException("Cannot find file " + _file.getAbsolutePath());
        }
        
        if(_file.isDirectory()) {
            throw new IOException("Cannot open directory " + _file.getAbsolutePath());
        }
        
        _raf = new RandomAccessFile(_file, "r");
        long size = Math.min(_raf.length(), Integer.MAX_VALUE);
        _mmapBuffer = _raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, size);
    }
    
    @Override
    public void close() throws IOException {
        try {
            _raf.close();
        } finally {
            _raf = null;
        }
    }
    
    @Override
    public int readInt() throws IOException {
        return _mmapBuffer.getInt();
    }
    
    @Override
    public long readLong() throws IOException {
        return _mmapBuffer.getLong();
    }
    
    @Override
    public short readShort() throws IOException {
        return _mmapBuffer.getShort();
    }
    
    @Override
    public int readInt(long position) throws IOException {
        return _mmapBuffer.getInt((int)position);
    }
    
    @Override
    public long readLong(long position) throws IOException {
        return _mmapBuffer.getLong((int)position);
    }
    
    @Override
    public short readShort(long position) throws IOException {
        return _mmapBuffer.getShort((int)position);
    }
    
    @Override
    public long position() throws IOException {
        return _mmapBuffer.position();
    }
    
    @Override
    public void position(long newPosition) throws IOException {
        _mmapBuffer.position((int)newPosition);
    }
}
