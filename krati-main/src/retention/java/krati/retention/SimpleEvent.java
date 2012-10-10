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

package krati.retention;

import krati.retention.clock.Clock;

/**
 * SimpleEvent
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 07/28, 2011 - Created
 */
public class SimpleEvent<T> implements Event<T> {
    private final static long serialVersionUID = 1L;
    private final Clock _clock;
    private final T _value;
    
    /**
     * Creates a new instance of SimpleEvent.
     * 
     * @param value - the event value
     * @param clock - the event clock
     */
    public SimpleEvent(T value, Clock clock) {
        this._value = value;
        this._clock = clock;
    }
    
    @Override
    public T getValue() {
        return _value;
    }
    
    @Override
    public Clock getClock() {
        return _clock;
    }
    
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName()).append("{");
        b.append("value=").append(_value).append(",");
        b.append("clock=").append(_clock).append("}");
        return b.toString();
    }
}
