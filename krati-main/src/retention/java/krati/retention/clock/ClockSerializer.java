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

import krati.io.SerializationException;
import krati.io.Serializer;

/**
 * ClockSerializer
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/11, 2011 - Created <br/>
 */
public final class ClockSerializer implements Serializer<Clock> {
    
    @Override
    public Clock deserialize(byte[] bytes) throws SerializationException {
        return Clock.parseClock(bytes);
    }
    
    @Override
    public byte[] serialize(Clock clock) throws SerializationException {
        return clock == null ? null : clock.toByteArray();
    }
}
