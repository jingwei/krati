package krati.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * MultiMappedWriter (not thread safe)
 * 
 * This class uses more than one MappedByteBuffer of size 128 MB to write to files larger than 2 GB.
 * 
 * @author jwu
 * 02/27, 2011
 * 
 * <p>
 * 06/09, 2011 - flush via FileChannel.force to boost performance.
 */
public class MultiMappedWriter implements DataWriter, BasicIO {
    private final File _file;
    private long _currentPosition;
    private FileChannel _channel;
    private RandomAccessFile _raf;
    private MappedByteBuffer[] _mmapArray;
    
    public final static int BUFFER_BITS = 27;
    public final static int BUFFER_SIZE = 1 << BUFFER_BITS; // 128 MB
    public final static long BUFFER_MASK = BUFFER_SIZE - 1;
    
    /**
     * Create a file writer based on MappedByteBuffer.
     *   
     * @param file - file to write to.
     */
    public MultiMappedWriter(File file) {
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
            File dir = _file.getParentFile();
            if(dir.exists())  _file.createNewFile();
            else if(dir.mkdirs()) _file.createNewFile();
            else throw new IOException("Failed to create file " + _file.getAbsolutePath());
        }
        
        if(_file.isDirectory()) {
            throw new IOException("Cannot open directory " + _file.getAbsolutePath());
        }
        
        // Create random access file
        _raf = new RandomAccessFile(_file, "rw");
        _channel = _raf.getChannel();

        // Allocate mapped buffer array
        int cnt = 0;
        long position = 0;
        
        cnt = (int)(_raf.length() >> BUFFER_BITS);
        cnt += (_raf.length() & BUFFER_MASK) > 0 ? 1 : 0;
        _mmapArray = new MappedByteBuffer[cnt];
        
        // Create individual mapped buffer
        for(int i = 0; i < cnt; i++) {
            long size = Math.min(_raf.length() - position, BUFFER_SIZE);
            _mmapArray[i] = _channel.map(FileChannel.MapMode.READ_WRITE, position, size);
            position += BUFFER_SIZE;
        }
        
        // Set current position to 0
        _currentPosition = 0;
    }
    
    @Override
    public void close() throws IOException {
        try {
            for(MappedByteBuffer b : _mmapArray) {
                if(b != null) b.force();
            }
            _channel.force(true);
            _channel.close();
            _raf.close();
        } finally {
            _currentPosition = 0;
            _mmapArray = null;
            _channel = null;
            _raf = null;
        }
    }
    
    @Override
    public void flush() throws IOException {
        _channel.force(true);
    }
    
    @Override
    public void force() throws IOException {
        for(MappedByteBuffer b : _mmapArray) {
            if(b != null) b.force();
        }
        _channel.force(true);
    }
    
    @Override
    public void writeInt(int value) throws IOException {
        int pos = (int)(_currentPosition & BUFFER_MASK);
        int ind = (int)(_currentPosition >> BUFFER_BITS);
        _mmapArray[ind].putInt(pos, value);
        _currentPosition += 4;
    }
    
    @Override
    public void writeLong(long value) throws IOException {
        int pos = (int)(_currentPosition & BUFFER_MASK);
        int ind = (int)(_currentPosition >> BUFFER_BITS);
        _mmapArray[ind].putLong(pos, value);
        _currentPosition += 8;
    }
    
    @Override
    public void writeShort(short value) throws IOException {
        int pos = (int)(_currentPosition & BUFFER_MASK);
        int ind = (int)(_currentPosition >> BUFFER_BITS);
        _mmapArray[ind].putShort(pos, value);
        _currentPosition += 2;
    }
    
    @Override
    public void writeInt(long position, int value) throws IOException {
        int pos = (int)(position & BUFFER_MASK);
        int ind = (int)(position >> BUFFER_BITS);
        _mmapArray[ind].putInt(pos, value);
    }
    
    @Override
    public void writeLong(long position, long value) throws IOException {
        int pos = (int)(position & BUFFER_MASK);
        int ind = (int)(position >> BUFFER_BITS);
        _mmapArray[ind].putLong(pos, value);
    }
    
    @Override
    public void writeShort(long position, short value) throws IOException {
        int pos = (int)(position & BUFFER_MASK);
        int ind = (int)(position >> BUFFER_BITS);
        _mmapArray[ind].putShort(pos, value);
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
}
