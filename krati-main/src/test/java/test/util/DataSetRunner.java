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

package test.util;

import java.util.List;
import java.util.Random;

import krati.store.DataSet;
import test.LatencyStats;

/**
 * DataSetRunner
 * 
 * @author jwu
 * 
 */
public abstract class DataSetRunner implements Runnable {
    DataSet<byte[]> _store;
    Random _rand = new Random();
    boolean _running = true;
    long _cnt = 0;
    
    LatencyStats _latStats = new LatencyStats();
    final List<String> _lineSeedData;
    final int _lineSeedCount;
    final int _keyCount;
    
    public DataSetRunner(DataSet<byte[]> store, List<String> seedData, int keyCount) {
        this._store = store;
        this._lineSeedData = seedData;
        this._lineSeedCount = seedData.size();
        this._keyCount = keyCount;
    }
    
    public long getOpCount() {
        return this._cnt;
    }
    
    public LatencyStats getLatencyStats() {
        return this._latStats;
    }
    
    public void stop() {
        _running = false;
    }
    
    protected abstract void op();
    
    @Override
    public void run() {
        long prevTime = System.nanoTime();
        long currTime = prevTime;
        
        while (_running) {
            op();
            _cnt++;
            
            currTime = System.nanoTime();
            _latStats.countLatency((int)(currTime - prevTime)/1000);
            prevTime = currTime;
        }
    }
}
