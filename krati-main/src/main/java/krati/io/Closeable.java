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

import java.io.IOException;

/**
 * Closeable
 * 
 * @author jwu
 * @since  04/21, 2011
 */
public interface Closeable extends java.io.Closeable {

    /**
     * @return <code>true</code> if the underlying service is open. Otherwise, <code>false</code>.
     */
    public boolean isOpen();
    
    /**
     * Open to start serving requests. If the service is already opened then invoking this 
     * method has no effect.
     * 
     * @throws IOException if the underlying service cannot be opened properly.
     */
    public void open() throws IOException;
    
    /**
     * Close to quit from serving requests. If the service is already closed then invoking this 
     * method has no effect.
     * 
     * @throws IOException if the underlying service cannot be closed properly.
     */
    @Override
    public void close() throws IOException;
}
