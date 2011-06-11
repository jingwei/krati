package krati.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A simple data writer based on FileChannel.
 * 
 * @author jwu
 *
 */
public class ChannelWriter implements DataWriter {
    private final File _file;
    private FileChannel _channel;
    private RandomAccessFile _raf;
    
    private final ByteBuffer _bbInt = ByteBuffer.wrap(new byte[4]);
    private final ByteBuffer _bbLong = ByteBuffer.wrap(new byte[8]);
    private final ByteBuffer _bbShort = ByteBuffer.wrap(new byte[2]);
    
    public ChannelWriter(File file) {
        this._file = file;
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
        
        _raf = new RandomAccessFile(_file, "rw");
        _channel = _raf.getChannel();
    }
    
    @Override
    public void close() throws IOException {
        try {
            _channel.force(true);
            _channel.close();
            _raf.close();
        } finally {
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
        _channel.force(true);
    }
    
    @Override
    public void writeInt(int value) throws IOException {
        _bbInt.position(0);
        _bbInt.putInt(value);
        _bbInt.flip();
        _channel.write(_bbInt);
    }
    
    @Override
    public void writeLong(long value) throws IOException {
        _bbLong.position(0);
        _bbLong.putLong(value);
        _bbLong.flip();
        _channel.write(_bbLong);
    }
    
    @Override
    public void writeShort(short value) throws IOException {
        _bbShort.position(0);
        _bbShort.putShort(value);
        _bbShort.flip();
        _channel.write(_bbShort);
    }
    
    public void writeBytes(byte[] bytes) throws IOException {
        _channel.write(ByteBuffer.wrap(bytes));
    }
    
    @Override
    public void writeInt(long position, int value) throws IOException {
        _bbInt.position(0);
        _bbInt.putInt(value);
        _bbInt.flip();
        _channel.write(_bbInt, position);
    }
    
    @Override
    public void writeLong(long position, long value) throws IOException {
        _bbLong.position(0);
        _bbLong.putLong(value);
        _bbLong.flip();
        _channel.write(_bbLong, position);
    }
    
    @Override
    public void writeShort(long position, short value) throws IOException {
        _bbShort.position(0);
        _bbShort.putShort(value);
        _bbShort.flip();
        _channel.write(_bbShort, position);
    }
    
    public void writeBytes(long position, byte[] bytes) throws IOException {
        _channel.write(ByteBuffer.wrap(bytes), position);
    }
    
    @Override
    public long position() throws IOException {
        return _channel.position();
    }
    
    @Override
    public void position(long newPosition) throws IOException {
        _channel.position(newPosition);
    }
}
