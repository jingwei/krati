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

package test.driver.raw;

import java.util.List;
import java.util.Random;

import test.LatencyStats;
import test.driver.StoreReader;

/**
 * Read driver for data store.
 * 
 * @author jwu
 *
 * @param <S> Data Store
 */
public class BytesReadDriver<S> implements Runnable {
    protected final S _store;
    protected final StoreReader<S, byte[], byte[]> _reader;
    protected final LatencyStats _latStats = new LatencyStats();
    protected final Random _rand = new Random();
    protected final List<String> _lineSeedData;
    protected final int _lineSeedCount;
    protected final int _keyCount;

    volatile long _cnt = 0;
    volatile boolean _running = true;

    public BytesReadDriver(S store, StoreReader<S, byte[], byte[]> reader, List<String> lineSeedData, int keyCount) {
        this._store = store;
        this._reader = reader;
        this._lineSeedData = lineSeedData;
        this._lineSeedCount = lineSeedData.size();
        this._keyCount = keyCount;
    }
    
    public LatencyStats getLatencyStats() {
        return this._latStats;
    }
    
    public long getReadCount() {
        return this._cnt;
    }
    
    public void stop() {
        _running = false;
    }
    
    @Override
    public void run() {
        long prevTime = System.nanoTime();
        long currTime = prevTime;
        
        while (_running) {
            read();

            currTime = System.nanoTime();
            _latStats.countLatency((int)(currTime - prevTime)/1000);
            prevTime = currTime;
        }
    }
    
    protected void read() {
        int i = _rand.nextInt(_keyCount);
        String s = _lineSeedData.get(i%_lineSeedCount);
        String k = s.substring(0, 30) + i;
        _reader.get(_store, k.getBytes());
        _cnt++;
    }
}
