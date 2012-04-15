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

package test.store;

import krati.core.segment.SegmentFactory;

/**
 * TestDynamicStore using MappedSegment.
 * 
 * @author jwu
 *
 */
public class TestDynamicStoreMapped extends TestDynamicStore {
    @Override
    protected SegmentFactory getSegmentFactory() {
        return new krati.core.segment.MappedSegmentFactory();
    }
}
