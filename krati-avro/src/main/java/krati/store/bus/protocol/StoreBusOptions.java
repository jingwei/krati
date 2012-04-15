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

package krati.store.bus.protocol;

import org.apache.avro.util.Utf8;

/**
 * StoreBusOptions
 * 
 * @author jwu
 * @since 10/16, 2011
 */
public class StoreBusOptions {
    public static final String OPT_NO_CLOCK = "--no-clock";
    public static final Utf8   OPT_NO_CLOCK_UTF8 = new Utf8(OPT_NO_CLOCK);
}
