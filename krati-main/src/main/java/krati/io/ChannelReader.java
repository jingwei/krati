/*
 * Copyright (c) 2010-2011 LinkedIn, Inc
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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A simple data reader based on FileChannel.
 * 
 * @author jwu
 *
 */
public class ChannelReader implements DataReader {
    private final File _file;
    private FileChannel _channel;
    private RandomAccessFile _raf;
    
    private final ByteBuffer _bbInt = ByteBuffer.wrap(new byte[4]);
    private final ByteBuffer _bbLong = ByteBuffer.wrap(new byte[8]);
    private final ByteBuffer _bbShort = ByteBuffer.wrap(new byte[2]);
    
    public ChannelReader(File file) {
        this._file = file;
    }
    
    @Override
    public File getFile() {
        return _file;
    }
    
    @Override
    public void open() throws IOException {
        if(!_file.exists()) {
            throw new IOException("Cannot find file " + _file.getAbsolutePath());
        }
        
        if(_file.isDirectory()) {
            throw new IOException("Cannot open directory " + _file.getAbsolutePath());
        }
        
        _raf = new RandomAccessFile(_file, "r");
        _channel = _raf.getChannel();
    }
    
    @Override
    public void close() throws IOException {
        try {
            if(_channel != null) _channel.close();
            if(_raf != null) _raf.close();
        } finally {
            _channel = null;
            _raf = null;
        }
    }
    
    @Override
    public int readInt() throws IOException {
        _bbInt.clear();
        _channel.read(_bbInt);
        _bbInt.flip();
        return _bbInt.getInt();
    }
    
    @Override
    public long readLong() throws IOException {
        _bbLong.clear();
        _channel.read(_bbLong);
        _bbLong.flip();
        return _bbLong.getLong();
    }
    
    @Override
    public short readShort() throws IOException {
        _bbShort.clear();
        _channel.read(_bbShort);
        _bbShort.flip();
        return _bbShort.getShort();
    }
    
    @Override
    public int readInt(long position) throws IOException {
        _bbInt.clear();
        _channel.read(_bbInt, position);
        _bbInt.flip();
        return _bbInt.getInt();
    }
    
    @Override
    public long readLong(long position) throws IOException {
        _bbLong.clear();
        _channel.read(_bbLong, position);
        _bbLong.flip();
        return _bbLong.getLong();
    }
    
    @Override
    public short readShort(long position) throws IOException {
        _bbShort.clear();
        _channel.read(_bbShort, position);
        _bbShort.flip();
        return _bbShort.getShort();
    }
    
    @Override
    public long position() throws IOException {
        return _channel.position();
    }
    
    @Override
    public void position(long newPosition) throws IOException {
        _channel.position(newPosition);
    }
}
