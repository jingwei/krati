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

package test.core;

import java.io.File;

import krati.core.array.basic.DynamicConstants;
import krati.core.segment.SegmentFactory;
import krati.store.AbstractDataArray;
import krati.store.DynamicDataArray;

/**
 * TestDynamicDataArrayMapped
 * 
 * @author jwu
 * 
 */
public class TestDynamicDataArrayMapped extends EvalDataArray {

    @Override
    protected SegmentFactory createSegmentFactory() {
        return new krati.core.segment.MappedSegmentFactory();
    }

    @Override
    protected AbstractDataArray createDataArray(File homeDir) throws Exception {
        int initialLength;
        initialLength = Math.max(_idCount, 1 << DynamicConstants.SUB_ARRAY_BITS);
        
        DynamicDataArray dynArray =
            new DynamicDataArray(initialLength,
                                 homeDir,
                                 createSegmentFactory(),
                                 _segFileSizeMB);
        
        dynArray.set(initialLength, null, System.currentTimeMillis());
        
        if(initialLength < (1 << 20)) {
            dynArray.set(initialLength * 2, null, System.currentTimeMillis());
        }
        
        return dynArray;
    }
}
