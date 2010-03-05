package krati.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ChannelReader implements DataReader
{
    private final File _file;
    private FileChannel _channel;
    private RandomAccessFile _raf;
    
    private ByteBuffer _bbInt = ByteBuffer.wrap(new byte[4]);
    private ByteBuffer _bbLong = ByteBuffer.wrap(new byte[8]);
    private ByteBuffer _bbShort = ByteBuffer.wrap(new byte[2]);
    
    public ChannelReader(File file)
    {
        this._file = file;
    }
    
    @Override
    public File getFile()
    {
        return _file;
    }

    @Override
    public void open() throws IOException
    {
        if(!_file.exists())
        {
            throw new IOException("Cannot find file " + _file.getAbsolutePath());
        }
        
        if(_file.isDirectory())
        {
            throw new IOException("Cannot open directory " + _file.getAbsolutePath());
        }
        
        _raf = new RandomAccessFile(_file, "r");
        _channel = _raf.getChannel();
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            _channel.close();
            _raf.close();
        }
        finally
        {
            _channel = null;
            _raf = null;
        }
    }

    @Override
    public int readInt() throws IOException
    {
        _bbInt.clear();
        _channel.read(_bbInt);
        _bbInt.flip();
        return _bbInt.getInt();
    }

    @Override
    public long readLong() throws IOException
    {
        _bbLong.clear();
        _channel.read(_bbLong);
        _bbLong.flip();
        return _bbLong.getLong();
    }
    
    @Override
    public short readShort() throws IOException
    {
        _bbShort.clear();
        _channel.read(_bbShort);
        _bbShort.flip();
        return _bbShort.getShort();
    }
    
    public int readInt(long position) throws IOException
    {
        _bbInt.clear();
        _channel.read(_bbInt, position);
        _bbInt.flip();
        return _bbInt.getInt();
    }
    
    public long readLong(long position) throws IOException
    {
        _bbLong.clear();
        _channel.read(_bbLong, position);
        _bbLong.flip();
        return _bbLong.getLong();
    }
    
    public short readShort(long position) throws IOException
    {
        _bbShort.clear();
        _channel.read(_bbShort, position);
        _bbShort.flip();
        return _bbShort.getShort();
    }
    
    public long position() throws IOException
    {
        return _channel.position();
    }
    
    public void position(long newPosition) throws IOException
    {
        _channel.position(newPosition);
    }
}
