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
 * SimpleEventBatchHeader
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/01, 2011 - Created
 */
public class SimpleEventBatchHeader implements EventBatchHeader {
    private final int _version;
    private final int _size;
    private final long _origin;
    private final Clock _minClock;
    private final Clock _maxClock;
    private final long _creationTime;
    private final long _completionTime;
    
    public SimpleEventBatchHeader(int version,
                                  int size,
                                  long origin,
                                  long creationTime,
                                  long completionTime,  
                                  Clock minClock, Clock maxClock) {
        this._version = version;
        this._size = size;
        this._origin = origin;
        this._minClock = minClock;
        this._maxClock = maxClock;
        this._creationTime = creationTime;
        this._completionTime = completionTime;
    }
    
    @Override
    public int getVersion() {
        return _version;
    }
    
    @Override
    public int getSize() {
        return _size;
    }
    
    @Override
    public long getOrigin() {
        return _origin;
    }
    
    @Override
    public long getCreationTime() {
        return _creationTime;
    }
    
    @Override
    public long getCompletionTime() {
        return _completionTime;
    }
    
    @Override
    public Clock getMinClock() {
        return _minClock;
    }
    
    @Override
    public Clock getMaxClock() {
        return _maxClock;
    }
    
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append(EventBatchHeader.class.getSimpleName()).append("{");
        b.append("version=").append(_version).append(",");
        b.append("size=").append(_size).append(",");
        b.append("origin=").append(_origin).append(",");
        b.append("creationTime=").append(_creationTime).append(",");
        b.append("completionTime=").append(_completionTime).append(",");
        b.append("minClock=").append(_minClock).append(",");
        b.append("maxClock=").append(_maxClock).append("}");
        return b.toString();
    }
}
