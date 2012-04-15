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

package krati.core.array;

import krati.Persistable;
import krati.array.LongArray;
import krati.core.array.entry.EntryPersistListener;
import krati.io.Closeable;

/**
 * AddressArray is for maintaining address pointers (long values) to data stored in segments. 
 * 
 * @author jwu
 *
 */
public interface AddressArray extends LongArray, Persistable, Closeable {
    
    /**
     * Gets the listener that is called whenever an entry is persisted. 
     */
    public EntryPersistListener getPersistListener();
    
    /**
     * Sets the listener that is called whenever an entry is persisted.
     * @param persistListener
     */
    public void setPersistListener(EntryPersistListener persistListener);
    
    /**
     * Sets the compaction address (produced by a compactor) at a specified index.
     * 
     * @param index   - the index to address array
     * @param address - the address value for update
     * @param scn     - the scn associated with this change
     */
    public void setCompactionAddress(int index, long address, long scn) throws Exception;
    
    /**
     * Expands the capacity of this AddressArray to accommodate a given index.
     * 
     * @param index an index in the array
     * @throws Exception
     */
    public void expandCapacity(int index) throws Exception;
}
