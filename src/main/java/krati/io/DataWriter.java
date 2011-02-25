package krati.io;

import java.io.File;
import java.io.IOException;

/**
 * DataWriter
 * 
 * @author jwu
 *
 */
public interface DataWriter {
    
    public File getFile();
    
    public void open() throws IOException;
    
    public void close() throws IOException;
    
    public void flush() throws IOException;
    
    public void writeInt(int value) throws IOException;
    
    public void writeLong(long value) throws IOException;
    
    public void writeShort(short value) throws IOException;
    
    public void writeInt(long position, int value) throws IOException;
    
    public void writeLong(long position, long value) throws IOException;
    
    public void writeShort(long position, short value) throws IOException;
    
    public long position() throws IOException;
    
    public void position(long newPosition) throws IOException;
}
