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

package krati.retention.clock;

/**
 * Occurred defines the result of comparing two Clocks <tt>c1</tt> and <tt>c2</tt>
 * 
 * <pre>
 *   c1 occurred EQUICONCURRENTLY to c2
 *   c1 occurred CONCURRENTLY to c2
 *   c1 occurred BEFORE c2
 *   c1 occurred AFTER c2
 * </pre>
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 09/27, 2011 - Created <br/>
 */
public enum Occurred {
    EQUICONCURRENTLY,
    CONCURRENTLY,
    BEFORE,
    AFTER;
}
