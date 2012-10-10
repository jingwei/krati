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

package test.store.handler;

import krati.io.serializer.LongSerializer;
import krati.store.DataStoreHandler;
import krati.store.handler.VK2LongDataStoreHandler;

/**
 * TestVK2LongDataStoreHandler
 * 
 * @author jwu
 * @since 08/19, 2012
 */
public class TestVK2LongDataStoreHandler extends AbstractTestDataStoreHandler {
    protected int keyLen;
    protected long valueStart;
    protected LongSerializer serializer;
    
    @Override
    protected void setUp() {
        keyLen = rand.nextInt(10);
        keyLen++;
        
        valueStart = rand.nextLong();
        serializer = new LongSerializer();
    }
    
    protected byte[] nextKey() {
        return randomBytes(keyLen++);
    }
    
    protected byte[] nextValue() {
        return serializer.serialize(valueStart++);
    }
    
    protected DataStoreHandler createDataStoreHandler() {
        return new VK2LongDataStoreHandler();
    }
}
