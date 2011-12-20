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

package krati.store;

import krati.Persistable;
import krati.array.DataArray;
import krati.io.Closeable;

/**
 * ArrayStore
 * 
 * @author jwu
 * 01/10, 2011
 *
 */
public interface ArrayStore extends Persistable, Closeable, DataArray {
    
    /**
     * @return the capacity of this ArrayStore.
     */
    public int capacity();
    
    /**
     * @return the index start of this ArrayStore.
     */
    public int getIndexStart();
    
    /**
     * Deletes data at an index;
     * 
     * @param index       - Index where data is to be removed.
     * @param scn         - System change number.
     * @throws Exception
     */
    public void delete(int index, long scn) throws Exception;
}
