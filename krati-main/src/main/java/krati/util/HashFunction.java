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

package krati.util;

/**
 * HashFunction
 * 
 * A hash function for mapping bytes to long
 * 
 * @author jwu
 *
 */
public interface HashFunction<K> {
    public long hash(K key);
    public static final long NON_HASH_CODE = 0;
    public static final long MIN_HASH_CODE = Long.MIN_VALUE;
    public static final long MAX_HASH_CODE = Long.MAX_VALUE;
}
