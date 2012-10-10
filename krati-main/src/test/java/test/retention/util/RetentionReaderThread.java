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

package test.retention.util;

import java.util.ArrayList;
import java.util.List;

import krati.retention.Event;
import krati.retention.Position;
import krati.retention.Retention;

/**
 * RetentionReaderThread
 * 
 * @author jwu
 * @since 02/10, 2012
 */
public class RetentionReaderThread<T> extends Thread {
    private volatile int _readCount = 0;
    private volatile long _stopOffset = Long.MAX_VALUE;
    private final Retention<T> _retention;
    
    public RetentionReaderThread(Retention<T> retention) {
        this._retention = retention;
    }
    
    public void stop(long offset) {
        _stopOffset = offset;
    }
    
    public int getReadCount() {
        return _readCount;
    }
    
    @Override
    public void run() {
        Position pos = _retention.getPosition(_retention.getClock(0));
        List<Event<T>> list = new ArrayList<Event<T>>();
        
        try {
            while(pos == null || pos.getOffset() < _stopOffset) {
                if(pos != null) {
                    pos = _retention.get(pos, list);
                    _readCount += list.size();
                    list.clear();
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}