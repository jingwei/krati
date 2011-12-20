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

package krati.core;

import java.io.File;
import java.io.IOException;

/**
 * StorePartitionConfig
 * 
 * @author jwu
 * @since 06/26, 2011
 * 
 */
public class StorePartitionConfig extends StoreConfig {
    private final int _partitionCount;
    private final int _partitionStart;
    private final int _partitionEnd;
    
    public StorePartitionConfig(File homeDir, int partitionStart, int partitionCount) throws IOException {
        super(homeDir, partitionStart, partitionCount);
        this._partitionStart = partitionStart;
        this._partitionCount = partitionCount;
        this._partitionEnd = partitionStart + partitionCount;
    }
    
    public final int getPartitionCount() {
        return _partitionCount;
    }
    
    public final int getPartitionStart() {
        return _partitionStart;
    }
    
    public final int getPartitionEnd() {
        return _partitionEnd;
    }
}
