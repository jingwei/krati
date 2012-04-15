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
 * Short Array
 * 
 * @author jwu
 *
 */
public interface ShortArray extends Array {
  /**
   * Gets data at a specified index.
   * 
   * @param index
   * @return data at a specified index
   */
  public short get(int index);
  
  /**
   * Sets data at a specified index.
   * 
   * @param index
   * @param value
   * @param scn
   */
  public void set(int index, short value, long scn) throws Exception;
  
  /**
   * Gets the internal primitive array.
   * 
   * @return short array.
   */
  public short[] getInternalArray();
}
