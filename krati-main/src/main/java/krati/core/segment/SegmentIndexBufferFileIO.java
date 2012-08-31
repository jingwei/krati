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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import krati.util.Bytes;

import org.apache.log4j.Logger;

/**
 * SegmentIndexBufferFileIO
 * 
 * @author jwu
 * @since 08/26, 2012
 */
public class SegmentIndexBufferFileIO implements SegmentIndexBufferIO {
    /**
     * The logger
     */
    private static final Logger _logger = Logger.getLogger(SegmentIndexBufferFileIO.class);
    
    /**
     * The storage version which is assigned 1.
     */
    public final static int STORAGE_VERSION = 1;
    
    /**
     * The storage version length (the first 4 bytes).
     */
    public final static int STORAGE_VERSION_LENGTH = Bytes.NUM_BYTES_IN_INT;
    
    /**
     * Reads from the specified segment index buffer file.
     *  
     * @param sib     - the segment index buffer
     * @param sibFile - the segment index buffer file to read from
     * @throws IOException
     */
    @Override
    public int read(SegmentIndexBuffer sib, File sibFile) throws IOException {
        check(sibFile);
        
        RandomAccessFile raf = new RandomAccessFile(sibFile, "r");
        FileChannel channel = raf.getChannel();
        
        readVersion(channel);
        int length = sib.read(channel);
        length += STORAGE_VERSION_LENGTH;
        
        channel.close();
        raf.close();
        
        if(_logger.isTraceEnabled()) {
            _logger.trace("read " + sibFile.getAbsolutePath());
        }
        
        return length;
    }
    
    /**
     * Writes to the specified segment index buffer file.
     * 
     * @param sib     - the segment index buffer
     * @param sibFile - the segment index buffer file to write to
     * @throws IOException
     */
    @Override
    public int write(SegmentIndexBuffer sib, File sibFile) throws IOException {
        create(sibFile);
        
        RandomAccessFile raf = new RandomAccessFile(sibFile, "rw");
        FileChannel channel = raf.getChannel();
        
        writeVersion(channel);
        int length = sib.write(channel);
        length += STORAGE_VERSION_LENGTH;
        
        raf.setLength(length);
        channel.force(true);
        channel.close();
        raf.close();
        
        if(_logger.isTraceEnabled()) {
            _logger.trace("write " + sibFile.getAbsolutePath());
        }
        
        return length;
    }
    
    /**
     * Reads version from readable channel.
     * 
     * @throws IOException
     */
    protected int readVersion(ReadableByteChannel channel) throws IOException {
        ByteBuffer version = ByteBuffer.allocate(STORAGE_VERSION_LENGTH);
        int len = channel.read(version);
        if (len < STORAGE_VERSION_LENGTH) {
            throw new IOException("Invalid Version");
        }
        return version.getInt(0);
    }
    
    /**
     * Writes the version to writable channel.
     * 
     * @throws IOException
     */
    protected void writeVersion(WritableByteChannel channel) throws IOException {
        ByteBuffer version = ByteBuffer.allocate(STORAGE_VERSION_LENGTH);
        version.putInt(STORAGE_VERSION);
        version.flip();
        channel.write(version);
    }
    
    /**
     * Creates the specified file if needed.
     * 
     * @throws IOException
     */
    protected void create(File file) throws IOException {
        if(!file.exists()) {
            File dir = file.getParentFile();
            if(dir.exists()) file.createNewFile();
            else if(dir.mkdirs()) file.createNewFile();
            else throw new IOException("Failed to create " + file.getAbsolutePath());
        }
        
        if(file.isDirectory()) {
            throw new IOException("Cannot open directory " + file.getAbsolutePath());
        }
    }
    
    /**
     * Checks the existence of the specified file.
     * 
     * @throws IOException
     */
    protected void check(File file) throws IOException {
        if(!file.exists()) {
            throw new FileNotFoundException(file.getAbsolutePath());
        }
        
        if(file.isDirectory()) {
            throw new IOException("Cannot open directory " + file.getAbsolutePath());
        }
    }
}
