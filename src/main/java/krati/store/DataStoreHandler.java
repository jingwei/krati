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

import java.util.List;
import java.util.Map.Entry;

/**
 * DataStoreHandler
 * 
 * @author jwu
 * 
 */
public interface DataStoreHandler extends DataHandler {
    
    public byte[] assemble(byte[] key, byte[] value);
    
    public byte[] assemble(byte[] key, byte[] value, byte[] data);
    
    public int countCollisions(byte[] key, byte[] data);
    
    public byte[] extractByKey(byte[] key, byte[] data);
    
    public int removeByKey(byte[] key, byte[] data);
    
    public List<byte[]> extractKeys(byte[] data);
    
    public List<Entry<byte[], byte[]>> extractEntries(byte[] data);
    
    public byte[] assembleEntries(List<Entry<byte[], byte[]>> entries);
}
