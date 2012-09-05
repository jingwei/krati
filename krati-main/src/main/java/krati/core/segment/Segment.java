/*
 * Copyright (c) 2010-2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package krati.core.segment;

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
public interface Segment {
    /**
     * The segment storage version.
     */
    public final static long STORAGE_VERSION = 1;
    
    /**
     * The default segment compaction factor (0.5).
     */
    public final static double defaultSegmentCompactFactor = 0.5;
    
    /**
     * The default segment file size in MB (256).
     */
    public final static int defaultSegmentFileSizeMB = 256;
    
    /**
     * The maximum segment file size in MB (2048).
     */
    public final static int maxSegmentFileSizeMB = 2048;
    
    /**
     * The minimum segment file size in MB (8).
     */
    public final static int minSegmentFileSizeMB = 8;
    
    /**
     * The position (i.e. offset) for last forced time within segment.
     */
    public final static int posLastForcedTime = 0;
    
    /**
     * The position (i.e. offset) for storage version within segment.
     */
    public final static int posStorageVersion = 8;
    
    /**
     * The data section starts at offset 128.
     */
    public final static int dataStartPosition = 128;
    
    /**
     * @return the segment {@link Segment.Mode Mode}.
     */
    public Mode getMode();
    
    /**
     * @return the segment Id.
     */
    public int getSegmentId();
    
    /**
     * @return the segment file.
     */
    public File getSegmentFile();
    
    /**
     * @return the initial segment size measured in bytes.
     */
    public long getInitialSize();
    
    /**
     * @return the initial segment size measured in MB.
     */
    public int getInitialSizeMB();
    
    /**
     * @return the segment load measured in bytes.
     */
    public int getLoadSize();
    
    /**
     * @return the segment load factor (i.e., the percentage of segment in use).
     */
    public double getLoadFactor();
    
    /**
     * Increases the segment load size.
     * 
     * @param byteCnt - the number of bytes to add.
     */
    public void incrLoadSize(int byteCnt);
    
    /**
     * Decreases the segment load size.
     * 
     * @param byteCnt - the number of bytes to subtract.
     */
    public void decrLoadSize(int byteCnt);
    
    /**
     * Gets the segment append position.
     * 
     * @return the current append position of this segment.
     * @throws IOException
     */
    public long getAppendPosition() throws IOException;
    
    /**
     * Sets the segment append position.
     * 
     * @param pos - the new append position
     * @throws IOException
     */
    public void setAppendPosition(long pos) throws IOException;
    
    /**
     * Reads an integer value from the specified position.
     * 
     * @param pos - the position (i.e. offset) in segment
     * @return an integer value
     * @throws IOException
     */
    public int readInt(int pos) throws IOException;
    
    /**
     * Reads a long value from the specified position.
     * 
     * @param pos - the position (i.e. offset) in segment
     * @return a long value
     * @throws IOException
     */
    public long readLong(int pos) throws IOException;
    
    /**
     * Reads a short value from the specified position.
     * 
     * @param pos - the position (i.e. offset) in segment
     * @return a short value
     * @throws IOException
     */
    public short readShort(int pos) throws IOException;
    
    /**
     * Reads bytes from the specified position into a byte array.
     * 
     * @param pos - the segment position
     * @param dst - the destination byte array
     * @throws IOException
     */
    public void read(int pos, byte[] dst) throws IOException;
    
    /**
     * Reads bytes from the specified position into a byte array.
     * 
     * @param pos    - the segment position
     * @param dst    - the destination byte array
     * @param offset - the offset to the byte array
     * @param length - the number of bytes to read from the specified position
     * @throws IOException
     */
    public void read(int pos, byte[] dst, int offset, int length) throws IOException;
    
    /**
     * Appends an integer value.
     * 
     * @return the segment position at which the specified value is appended.
     * @throws IOException
     */
    public int appendInt(int value) throws IOException;
    
    /**
     * Appends a long value.
     * 
     * @return the segment position at which the specified value is appended.
     * @throws IOException
     */
    public int appendLong(long value) throws IOException;
    
    /**
     * Appends a short value.
     * 
     * @return the segment position at which the specified value is appended.
     * @throws IOException
     */
    public int appendShort(short value) throws IOException;
    
    /**
     * Appends the specified byte array.
     * 
     * @return the segment position at which the specified data is appended.
     * @throws IOException
     */
    public int append(byte[] data) throws IOException;
    
    /**
     * Appends bytes from the specified byte array.
     * 
     * @param data   - the byte array
     * @param offset - the offset to the byte array
     * @param length - the number of bytes from the byte array to append
     * @return the segment position at which the specified data is appended.
     * @throws IOException
     */
    public int append(byte[] data, int offset, int length) throws IOException;
    
    /**
     * Transfers bytes to a target segment.
     * 
     * @param pos           - the segment position
     * @param length        - the number of bytes to read from the specified position
     * @param targetSegment - the target segment to append to
     * @return the number of bytes transferred
     * @throws IOException
     */
    public int transferTo(int pos, int length, Segment targetSegment) throws IOException;
    
    /**
     * Transfers bytes to a target writable channel.
     * 
     * @param pos           - the segment position
     * @param length        - the number of bytes to read from the specified position
     * @param targetChannel - the target writable channel to append to
     * @return the number of bytes transferred
     * @throws IOException
     */
    public int transferTo(int pos, int length, WritableByteChannel targetChannel) throws IOException;
    
    /**
     * Tests if this segment is read-only.
     */
    public boolean isReadOnly();
    
    /**
     * Changes this segment to read-only.
     * 
     * @throws IOException
     */
    public void asReadOnly() throws IOException;
    
    /**
     * Forces updates to this segment to disk.
     * 
     * @throws IOException
     */
    public void force() throws IOException;
    
    /**
     * Closes this segment to disable read/write operations.
     * 
     * @param force - whether to force updates to this segment to disk 
     * @throws IOException
     */
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
     * @return whether this Segment can support reads from a buffer.
     */
    public boolean canReadFromBuffer();
    
    /**
     * @return whether this Segment can support writes by appending to a buffer.
     */
    public boolean canAppendToBuffer();
    
    /**
     * Re-initialize this Segment for read and write.
     */
    public void reinit() throws IOException;
    
    /**
     * @return the descriptive status of this Segment.
     */
    public String getStatus();
    
    /**
     * The segment mode.
     */
    public static enum Mode {
        READ_ONLY,
        READ_WRITE;
    }
}
