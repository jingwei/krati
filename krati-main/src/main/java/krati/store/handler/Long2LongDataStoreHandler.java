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

package krati.store.handler;

import krati.util.Bytes;

/**
 * Long2LongDataStoreHandler- the long key to long value {@link krati.store.DataStoreHandler DataStoreHandler}.
 * 
 * @author jwu
 * @since 08/18, 2012
 */
public final class Long2LongDataStoreHandler extends FKFVDataStoreHandler {
    
    /**
     * Creates a new instance of Long2IntDataStoreHandler.
     */
    public Long2LongDataStoreHandler() {
        super(Bytes.NUM_BYTES_IN_LONG, Bytes.NUM_BYTES_IN_LONG);
    }
}
