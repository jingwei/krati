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
 * BasicIO defines an interface for basic read/write IO operations. 
 * 
 * @author jwu
 * 06/12, 2011
 * 
 */
public interface BasicIO {
    
    /**
     * @return the file on which IO operations will be performed.
     */
    public File getFile();
    
    /**
     * Opens for read and write operations.
     * 
     * @throws IOException
     */
    public void open() throws IOException;
    
    /**
     * Closes for read and write operations.
     * 
     * @throws IOException
     */
    public void close() throws IOException;
    
    /**
     * Reads an integer value from the specified position in the underlying file.
     * 
     * @param position - file position
     * @return an integer value
     * @throws IOException
     */
    public int readInt(long position) throws IOException;
    
    /**
     * Reads a long value from the specified position in the underlying file.
     * 
     * @param position - file position
     * @return a long value
     * @throws IOException
     */
    public long readLong(long position) throws IOException;
    
    /**
     * Reads a short value from the specified position in the underlying file.
     * 
     * @param position - file position
     * @return a short value
     * @throws IOException
     */
    public short readShort(long position) throws IOException;
    
    /**
     * Writes an integer value at the specified position in the underlying file.
     * 
     * @param position - file position
     * @throws IOException
     */
    public void writeInt(long position, int value) throws IOException;
    
    /**
     * Writes a long value at the specified position in the underlying file.
     * 
     * @param position - file position
     * @throws IOException
     */
    public void writeLong(long position, long value) throws IOException;
    
    /**
     * Writes a short value at the specified position in the underlying file.
     * 
     * @param position - file position
     * @throws IOException
     */
    public void writeShort(long position, short value) throws IOException;
}
