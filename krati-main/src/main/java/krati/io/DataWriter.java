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

/**
 * DataWriter
 * 
 * @author jwu
 * 
 * <p>
 * 06/09, 2011 - Added a conservative version of flush force()
 */
public interface DataWriter {
    
    public File getFile();
    
    public void open() throws IOException;
    
    public void close() throws IOException;
    
    public void flush() throws IOException;
    
    public void force() throws IOException;
    
    public void writeInt(int value) throws IOException;
    
    public void writeLong(long value) throws IOException;
    
    public void writeShort(short value) throws IOException;
    
    public void writeInt(long position, int value) throws IOException;
    
    public void writeLong(long position, long value) throws IOException;
    
    public void writeShort(long position, short value) throws IOException;
    
    public long position() throws IOException;
    
    public void position(long newPosition) throws IOException;
}
