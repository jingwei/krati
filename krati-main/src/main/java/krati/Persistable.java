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

package krati;

import java.io.IOException;

/**
 * Persistable
 * 
 * @author jwu
 *
 */
public interface Persistable {
    /**
     * Force all updates from memory buffer and redo log files to synchronize with
     * the underlying persistent file in blocking mode.
     *  
     * @throws IOException
     */
    public void sync() throws IOException;
    
    /**
     * Persist all updates from memory buffer into redo log files in non-blocking mode.
     *  
     * @throws IOException
     */
    public void persist() throws IOException;
    
    /**
     * Gets the low water mark, below which all updates are persisted.
     */
    public long getLWMark();
    
    /**
     * Gets the high water mark, below which all updates are readable.
     */
    public long getHWMark();
    
    /**
     * Saves the high water mark to indicate the progress made so far.
     * 
     * @param endOfPeriod
     * @throws Exception
     */
    public void saveHWMark(long endOfPeriod) throws Exception;
}
