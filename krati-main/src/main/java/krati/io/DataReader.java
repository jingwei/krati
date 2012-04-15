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

/**
 * DataReader
 * 
 * @author jwu
 *
 */
public interface DataReader {
    
    public File getFile();
    
    public void open() throws IOException;
    
    public void close() throws IOException;
    
    public int readInt() throws IOException;
    
    public long readLong() throws IOException;
    
    public short readShort() throws IOException;
    
    public int readInt(long position) throws IOException;
    
    public long readLong(long position) throws IOException;
    
    public short readShort(long position) throws IOException;
    
    public long position() throws IOException;
    
    public void position(long newPosition) throws IOException;
}
