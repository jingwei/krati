package krati.cds.impl.segment;

import java.io.File;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * Segment Storage Version: 1
 * 
 * The segment header section uses the first 128 bytes, but only the first 16 bytes are used.
 * The segment data section stores varying-length data in the format of repeated [length][data ... ...].
 * 
 * Header Section:
 * <code>
 *   0x00  long    -- 8 bytes lastForcedTime
 *   0x08  long    -- 8 bytes storageVersion
 *   0x10  long    -- 8 bytes reserved
 *   0x18  long    -- 8 bytes reserved
 *   0x20  long    -- 8 bytes reserved
 *   0x28  long    -- 8 bytes reserved
 *   0x30  long    -- 8 bytes reserved
 *   0x38  long    -- 8 bytes reserved
 *   0x40  long    -- 8 bytes reserved
 *   0x48  long    -- 8 bytes reserved
 *   0x50  long    -- 8 bytes reserved
 *   0x58  long    -- 8 bytes reserved
 *   0x60  long    -- 8 bytes reserved
 *   0x68  long    -- 8 bytes reserved
 *   0x70  long    -- 8 bytes reserved
 *   0x78  long    -- 8 bytes reserved
 * </code>
 * 
 * Data Section:
 * <code>
 *   [length1][data1 ... ...]
 *   [length2][data2 ... ... ... ... ... ...]
 *   [length3][data3 ... ... ... ...]
 *   [length4][data4 ...]
 *   ...
 * </code>
 * 
 * @author jwu
 *
 */
public interface Segment
{
    public final static long STORAGE_VERSION = 1;
    
    public final static int defaultSegmentFileSizeMB = 512;
    public final static int maxSegmentFileSizeMB = 2048;
    public final static int minSegmentFileSizeMB = 32;
    
    public final static int posLastForcedTime = 0;
    public final static int posStorageVersion = 8;
    
    /**
     * The data section starts at offset 128.
     */
    public final static int dataStartPosition = 128;
    
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
     * @return the last forced time in milliseconds since the epoch, January 01, 1970, 00:00:00 GMT.
     */
    public long getLastForcedTime();
    
    /**
     * @return the storage version of this segment.
     */
    public long getStorageVersion();
    
    /**
     * @return whether this Segment is recyclable.
     */
    public boolean isRecyclable();
    
    /**
     * Re-initialize this Segment for read and write.
     */
    public void reinit() throws IOException;
    
    /**
     * @return the descriptive status of this Segment.
     */
    public String getStatus();
    
    public static enum Mode
    {
        READ_ONLY,
        READ_WRITE;
    }
}
