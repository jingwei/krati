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

/**
 * SegmentIndexBufferIO
 * 
 * @author jwu
 * @since 08/25, 2012
 */
public interface SegmentIndexBufferIO {
    
    /**
     * Reads from the specified segment index buffer file.
     *  
     * @param sib     - the segment index buffer
     * @param sibFile - the segment index buffer file to read from
     * @throws IOException
     */
    public int read(SegmentIndexBuffer sib, File sibFile) throws IOException;
    
    /**
     * Reads from the specified segment index buffer file and validates against
     * the specified <code>lastForcedTime</code> of segment index buffer.
     *  
     * @param sib               - the segment index buffer
     * @param sibFile           - the segment index buffer file to read from
     * @param sibLastForcedTime - the segment index buffer last forced time
     * 
     * @throws IOException
     * @throws SegmentIndexBufferException if the specified <code>sibLastForcedTime</code> is different from
     *         the value known to the specified segment index buffer file.
     */
    public int read(SegmentIndexBuffer sib, File sibFile, long sibLastForcedTime) throws IOException, SegmentIndexBufferException;
    
    /**
     * Writes to the specified segment index buffer file.
     * 
     * @param sib     - the segment index buffer
     * @param sibFile - the segment index buffer file to write to
     * @throws IOException
     */
    public int write(SegmentIndexBuffer sib, File sibFile) throws IOException;
}
