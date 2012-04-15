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

/**
 * DataSetHandler
 * 
 * @author jwu
 * 
 */
public interface DataSetHandler extends DataHandler {
    
    public int count(byte[] data);
    
    public byte[] assemble(byte[] value);
    
    public byte[] assemble(byte[] value, byte[] data);
    
    public int countCollisions(byte[] value, byte[] data);
    
    public int remove(byte[] value, byte[] data);
    
    public boolean find(byte[] value, byte[] data);
}
