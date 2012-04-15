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

package krati.core.array.entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * EntryValueBatch
 * 
 * @author jwu
 * 
 */
public abstract class EntryValueBatch {
    protected final int _valueSize;
    protected final int _valueCapacity;
    protected final ByteBuffer _buffer;
    
    protected EntryValueBatch(int valueSize, int valueCapacity) {
        this._valueSize = valueSize;
        this._valueCapacity = valueCapacity;
        this._buffer = ByteBuffer.allocate(_valueCapacity * _valueSize);
    }
    
    public int getCapacity() {
        return _valueCapacity;
    }
    
    public int getByteCapacity() {
        return _buffer.capacity();
    }
    
    public ByteBuffer getInternalBuffer() {
        return _buffer;
    }
    
    public void write(FileChannel channel) throws IOException {
        channel.write(ByteBuffer.wrap(_buffer.array(), 0, _buffer.position()));
    }
    
    public void clear() {
        _buffer.clear();
    }
}
