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

/**
 * IOFactory
 * 
 * @author jwu
 * @since  03/17, 2011
 * 
 */
public class IOFactory {    
    
    /**
     * Creates a new DataReader to read from a file.
     * 
     * @param file - file to read.
     * @param type - I/O type.
     * @return a new DataReader instance.
     */
    public final static DataReader createDataReader(File file, IOType type) {
        if(type == IOType.MAPPED) {
            if(file.length() <= Integer.MAX_VALUE) {
                return new MappedReader(file);
            } else {
                return new MultiMappedReader(file);
            }
        } else {
            return new ChannelReader(file);
        }
    }
    
    /**
     * Creates a new DataWriter to write to a file.
     * 
     * @param file - file to write.
     * @param type - I/O type.
     * @return a new DataWriter instance of type {@link BasicIO}.
     */
    public final static DataWriter createDataWriter(File file, IOType type) {
        if(type == IOType.MAPPED) {
            if(file.length() <= Integer.MAX_VALUE) {
                return new MappedWriter(file);
            } else {
                return new MultiMappedWriter(file);
            }
        } else {
            return new ChannelWriter(file);
        }
    }
}
