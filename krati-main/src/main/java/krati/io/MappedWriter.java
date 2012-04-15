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

package krati.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * MappedWriter
 * 
 * @author jwu
 * 
 * <p>
 * 06/08, 2011 - Revert to FileChannel.force to fix performance degradation introduced in 0.3.8
 */
public class MappedWriter implements DataWriter, BasicIO {
    private final File _file;
    private FileChannel _channel;
    private RandomAccessFile _raf;
    private MappedByteBuffer _mmapBuffer;
    
    public MappedWriter(File file) {
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
        _mmapBuffer = _channel.map(FileChannel.MapMode.READ_WRITE, 0, _raf.length());
    }
    
    @Override
    public void close() throws IOException {
        try {
            _mmapBuffer.force();
            _channel.force(true);
            _channel.close();
            _raf.close();
        } finally {
            _mmapBuffer = null;
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
        _mmapBuffer.force();
    }
    
    @Override
    public void writeInt(int value) throws IOException {
        _mmapBuffer.putInt(value);
    }
    
    @Override
    public void writeLong(long value) throws IOException {
        _mmapBuffer.putLong(value);
    }
    
    @Override
    public void writeShort(short value) throws IOException {
        _mmapBuffer.putShort(value);
    }
    
    @Override
    public void writeInt(long position, int value) throws IOException {
        _mmapBuffer.putInt((int)position, value);
    }
    
    @Override
    public void writeLong(long position, long value) throws IOException {
        _mmapBuffer.putLong((int)position, value);
    }
    
    @Override
    public void writeShort(long position, short value) throws IOException {
        _mmapBuffer.putShort((int)position, value);
    }
    
    @Override
    public long position() throws IOException {
        return _mmapBuffer.position();
    }
    
    @Override
    public void position(long newPosition) throws IOException {
        _mmapBuffer.position((int)newPosition);
    }
    
    @Override
    public int readInt(long position) throws IOException {
        return _mmapBuffer.getInt((int)position);
    }
    
    @Override
    public long readLong(long position) throws IOException {
        return _mmapBuffer.getLong((int)position);
    }
    
    @Override
    public short readShort(long position) throws IOException {
        return _mmapBuffer.getShort((int)position);
    }
}
