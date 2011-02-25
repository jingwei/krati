package krati.io;

import java.io.File;
import java.io.IOException;

/**
 * DataReader
 * 
 * @author jwu
 *
 */
public interface DataReader {
    
    public File getFile();
    
    public void open() throws IOException;
    
    public void close() throws IOException;
    
    public int readInt() throws IOException;
    
    public long readLong() throws IOException;
    
    public short readShort() throws IOException;
    
    public int readInt(long position) throws IOException;
    
    public long readLong(long position) throws IOException;
    
    public short readShort(long position) throws IOException;
    
    public long position() throws IOException;
    
    public void position(long newPosition) throws IOException;
}
