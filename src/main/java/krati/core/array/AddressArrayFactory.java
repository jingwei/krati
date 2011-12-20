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
    
    public AddressArrayFactory(boolean indexesCached) {
        this.setIndexesCached(indexesCached);
    }
    
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
    
    public AddressArray createDynamicAddressArray(File homeDir,
                                                  int batchSize,
                                                  int numSyncBatches) throws Exception {
        return createDynamicAddressArray(
                homeDir,
                DynamicConstants.SUB_ARRAY_SIZE,
                batchSize,
                numSyncBatches);
    }
    
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
    
    public final void setIndexesCached(boolean _indexesCached) {
        this._indexesCached = _indexesCached;
    }
    
    public final boolean getIndexesCached() {
        return _indexesCached;
    }
    
    public final boolean isIndexesCached() {
        return _indexesCached;
    }
}
