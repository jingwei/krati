package krati.mds.impl.segment;

import java.io.File;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * Segment
 * 
 * @author jwu
 *
 */
public interface Segment
{
    public final static int defaultSegmentFileSizeMB = 1024;
    public final static int maxSegmentFileSizeMB = 2048;
    public final static int minSegmentFileSizeMB = 32;
    public final static int dataStartPosition = 8;
    
    public Mode getMode();
    
    public int getSegmentId();
    
    public File getSegmentFile();
    
    public long getInitialSize();
    
    public int getInitialSizeMB();
    
    public int getLoadSize();
    
    public double getLoadFactor();
    
    public void incrLoadSize(int byteCnt);
    
    public void decrLoadSize(int byteCnt);
    
    public long getAppendPosition() throws IOException;
    
    public void setAppendPosition(long pos) throws IOException;
    
    public int readInt(int pos) throws IOException;
    
    public long readLong(int pos) throws IOException;
    
    public short readShort(int pos) throws IOException;
    
    public void read(int pos, byte[] dst) throws IOException;
    
    public void read(int pos, byte[] dst, int offset, int length) throws IOException;
    
    public int appendInt(int value) throws IOException;
    
    public int appendLong(long value) throws IOException;
    
    public int appendShort(short value) throws IOException;
    
    public int append(byte[] data) throws IOException;
    
    public int append(byte[] data, int offset, int length) throws IOException;
    
    public long transferTo(long pos, int length, WritableByteChannel targetChannel) throws IOException;
    
    public boolean isReadOnly();
    
    public void asReadOnly() throws IOException;
    
    public void load() throws IOException;
    
    public void force() throws IOException;
    
    public void close(boolean force) throws IOException;
    
    /**
     * @return last forced time in milliseconds since the epoch, January 01, 1970, 00:00:00 GMT.
     */
    public long getLastForcedTime();
    
    public static enum Mode
    {
        READ_ONLY,
        READ_WRITE;
    }
}
