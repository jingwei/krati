package krati.io;

import java.io.File;
import java.io.IOException;

public interface DataWriter
{
    public File getFile();
    
    public void open() throws IOException;
    
    public void close() throws IOException;

    public void flush() throws IOException;
    
    public void writeInt(int value) throws IOException;
    
    public void writeLong(long value) throws IOException;
    
    public void writeShort(short value) throws IOException;
}
