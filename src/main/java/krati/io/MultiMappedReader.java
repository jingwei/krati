package krati.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * MultiMappedReader (not thread safe)
 * 
 * This class uses more than one MappedByteBuffer of size 128 MB to read from files larger than 2 GB.
 * 
 * @author jwu
 * 02/26, 2011
 * 
 */
public class MultiMappedReader implements DataReader {
    private final File _file;
    private long _currentPosition;
    private RandomAccessFile _raf;
    private MappedByteBuffer[] _mmapArray;
    
    public final static int BUFFER_BITS = 27;
    public final static int BUFFER_SIZE = 1 << BUFFER_BITS; // 128 MB
    public final static long BUFFER_MASK = BUFFER_SIZE - 1;
    
    /**
     * Create a file reader based on MappedByteBuffer.
     *   
     * @param file - file to read from.
     */
    public MultiMappedReader(File file) {
        this._file = file;
        this._currentPosition = 0;
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
        
        // Create random access file
        _raf = new RandomAccessFile(_file, "r");
        
        // Allocate mapped buffer array
        int cnt = 0;
        long position = 0;
        
        cnt = (int)(_raf.length() >> BUFFER_BITS);
        cnt += (_raf.length() & BUFFER_MASK) > 0 ? 1 : 0;
        _mmapArray = new MappedByteBuffer[cnt];
        
        // Create individual mapped buffer
        for(int i = 0; i < cnt; i++) {
            long size = Math.min(_raf.length() - position, BUFFER_SIZE);
            _mmapArray[i] = _raf.getChannel().map(FileChannel.MapMode.READ_ONLY, position, size);
            position += BUFFER_SIZE;
        }
        
        // Set current position to 0
        _currentPosition = 0;
    }
    
    @Override
    public void close() throws IOException {
        try {
            _raf.close();
        } finally {
            _raf = null;
            _mmapArray = null;
            _currentPosition = 0;
        }
    }
    
    @Override
    public int readInt() throws IOException {
        int pos = (int)(_currentPosition & BUFFER_MASK);
        int ind = (int)(_currentPosition >> BUFFER_BITS);
        int val = _mmapArray[ind].getInt(pos);
        
        _currentPosition += 4;
        return val;
    }
    
    @Override
    public long readLong() throws IOException {
        int pos = (int)(_currentPosition & BUFFER_MASK);
        int ind = (int)(_currentPosition >> BUFFER_BITS);
        long val = _mmapArray[ind].getLong(pos);
        
        _currentPosition += 8;
        return val;
    }
    
    @Override
    public short readShort() throws IOException {
        int pos = (int)(_currentPosition & BUFFER_MASK);
        int ind = (int)(_currentPosition >> BUFFER_BITS);
        short val = _mmapArray[ind].getShort(pos);
        
        _currentPosition += 2;
        return val;
    }
    
    @Override
    public int readInt(long position) throws IOException {
        int pos = (int)(position & BUFFER_MASK);
        int ind = (int)(position >> BUFFER_BITS);
        return _mmapArray[ind].getInt(pos);
    }
    
    @Override
    public long readLong(long position) throws IOException {
        int pos = (int)(position & BUFFER_MASK);
        int ind = (int)(position >> BUFFER_BITS);
        return _mmapArray[ind].getLong(pos);
    }
    
    @Override
    public short readShort(long position) throws IOException {
        int pos = (int)(position & BUFFER_MASK);
        int ind = (int)(position >> BUFFER_BITS);
        return _mmapArray[ind].getShort(pos);
    }
    
    @Override
    public long position() throws IOException {
        return _currentPosition;
    }
    
    @Override
    public void position(long newPosition) throws IOException {
        int pos = (int)(newPosition & BUFFER_MASK);
        int ind = (int)(newPosition >> BUFFER_BITS);
        
        for(int i = 0; i < ind; i++) {
            _mmapArray[i].position((int)BUFFER_MASK);
        }
        
        _mmapArray[ind].position(pos);
        
        for(int i = ind; i < _mmapArray.length; i++) {
            _mmapArray[i].clear();
        }
        
        _currentPosition = newPosition;
    }
}