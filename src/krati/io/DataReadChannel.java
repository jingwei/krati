package krati.io;

import java.io.File;
import java.io.IOException;

public interface DataReadChannel
{
    public File getFile();
    
    public void open() throws IOException;
    
    public void close() throws IOException;
    
    public int readInt() throws IOException;
    
    public long readLong() throws IOException;
    
    public short readShort() throws IOException;
}
