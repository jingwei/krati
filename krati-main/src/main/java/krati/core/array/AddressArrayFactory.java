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

import java.io.File;

import krati.array.Array;
import krati.core.StoreParams;
import krati.core.array.basic.DynamicConstants;
import krati.core.array.basic.DynamicLongArray;
import krati.core.array.basic.IOTypeLongArray;
import krati.core.array.basic.StaticLongArray;

/**
 * AddressArrayFactory
 * 
 * @author jwu
 * 06/22, 2011
 * 
 */
public class AddressArrayFactory {
    private boolean _indexesCached = StoreParams.INDEXES_CACHED_DEFAULT;
    
    /**
     * Creates a new instance of AddressArrayFactory.
     * 
     * @param indexesCached - whether the indexes is cached in memory.
     */
    public AddressArrayFactory(boolean indexesCached) {
        this.setIndexesCached(indexesCached);
    }
    
    /**
     * Creates a fixed-length {@link AddressArray}.
     * 
     * @param homeDir        - the home directory where the <code>indexes.dat</code> is located.
     * @param length         - the length of {@link AddressArray}.
     * @param batchSize      - the number of updates per update batch.
     * @param numSyncBatches - the number of update batches required for updating the underlying indexes.
     * @return an instance of {@link AddressArray}.
     * @throws Exception if an instance of {@link AddressArray} cannot be created.
     */
    public AddressArray createStaticAddressArray(File homeDir,
                                                 int length,
                                                 int batchSize,
                                                 int numSyncBatches) throws Exception {
        AddressArray addrArray;
        
        if(_indexesCached) {
            addrArray = new StaticLongArray(length, batchSize, numSyncBatches, homeDir);
        } else {
            addrArray = new IOTypeLongArray(
                    Array.Type.STATIC,
                    length, batchSize, numSyncBatches, homeDir);
        }
        
        return addrArray;
    }
    
    /**
     * Creates a dynamic {@link AddressArray} which grows its capacity as needed.
     * The initial capacity of this address array is 64K.
     * 
     * @param homeDir        - the home directory where the <code>indexes.dat</code> is located.
     * @param batchSize      - the number of updates per update batch.
     * @param numSyncBatches - the number of update batches required for updating the underlying indexes.
     * @return an instance of {@link AddressArray}.
     * @throws Exception if an instance of {@link AddressArray} cannot be created.
     */
    public AddressArray createDynamicAddressArray(File homeDir,
                                                  int batchSize,
                                                  int numSyncBatches) throws Exception {
        return createDynamicAddressArray(
                homeDir,
                DynamicConstants.SUB_ARRAY_SIZE,
                batchSize,
                numSyncBatches);
    }
    
    /**
     * Creates a dynamic {@link AddressArray} which grows its capacity as needed.
     * 
     * @param homeDir        - the home directory where the <code>indexes.dat</code> is located.
     * @param initialLength  - the initial length of the created {@link AddressArray}.
     * @param batchSize      - the number of updates per update batch.
     * @param numSyncBatches - the number of update batches required for updating the underlying indexes.
     * @return an instance of {@link AddressArray}.
     * @throws Exception if an instance of {@link AddressArray} cannot be created.
     */
    public AddressArray createDynamicAddressArray(File homeDir,
                                                  int initialLength,
                                                  int batchSize,
                                                  int numSyncBatches) throws Exception {
        AddressArray addrArray;
        
        if (_indexesCached) {
            addrArray = new DynamicLongArray(batchSize, numSyncBatches, homeDir);
        } else {
            addrArray = new IOTypeLongArray(
                    Array.Type.DYNAMIC, initialLength,
                    batchSize, numSyncBatches, homeDir);
        }
        
        if(addrArray.length() < initialLength) {
            addrArray.expandCapacity(initialLength - 1);
        }
        
        return addrArray;
    }
    
    /**
     * Indicates whether this AddressArrayFactory creates an {@link AddressArray} with the indexes cached in memory.
     * 
     * @param indexesCached - whether the indexes is cached in memory.
     */
    public final void setIndexesCached(boolean indexesCached) {
        this._indexesCached = indexesCached;
    }
    
    /**
     * Checks whether the indexes is cached in memory.
     */
    public final boolean getIndexesCached() {
        return _indexesCached;
    }
    
    /**
     * Checks whether the indexes is cached in memory.
     */
    public final boolean isIndexesCached() {
        return _indexesCached;
    }
}
