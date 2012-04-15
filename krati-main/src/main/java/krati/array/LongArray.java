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

package krati.array;

/**
 * Long Array
 * 
 * @author jwu
 *
 */
public interface LongArray extends Array {
  /**
   * Gets data at a specified index.
   * 
   * @param index
   * @return data at a specified index
   */
  public long get(int index);
  
  /**
   * Sets data at a specified index.
   * 
   * @param index
   * @param value
   * @param scn
   */
  public void set(int index, long value, long scn) throws Exception;
  
  /**
   * Gets the internal primitive array.
   * 
   * @return long array.
   */
  public long[] getInternalArray();
}
