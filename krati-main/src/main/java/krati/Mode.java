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

/**
 * Mode defines the states of a store. 
 * 
 * @author jwu
 * @since 05/08, 2011
 */
public enum Mode {
    /**
     * Store is being initialized. 
     */
    INIT,
    
    /**
     * Store is ready for read, write and other operations.
     */
    OPEN,
    
    /**
     * Store is ready for read-only operations.
     */
    OPEN_FOR_READ,
    
    /**
     * Store is ready for write-only operations.
     */
    OPEN_FOR_WRITE,
    
    /**
     * Store is ready for read and write operations.
     */
    OPEN_FOR_READ_WRITE,
    
    /**
     * Store is closed for read, write and other operations.
     */
    CLOSED;
}
