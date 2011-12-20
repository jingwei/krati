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

package test.util;

import java.util.List;
import java.util.Random;

import krati.array.DataArray;
import test.LatencyStats;

/**
 * DataArrayReader
 * 
 * @author jwu
 * 
 */
public class DataArrayReader implements Runnable {
    DataArray _dataArray;
    Random _rand = new Random();
    byte[] _data = new byte[1 << 13];
    boolean _running = true;
    long _cnt = 0;
    
    LatencyStats _latStats = new LatencyStats();
    final List<String> _lineSeedData;
    
    public DataArrayReader(DataArray dataArray, List<String> seedData) {
        this._dataArray = dataArray;
        this._lineSeedData = seedData;
    }
    
    public long getReadCount() {
        return this._cnt;
    }
    
    public LatencyStats getLatencyStats() {
        return this._latStats;
    }
    
    public void stop() {
        _running = false;
    }
    
    int read(int index) {
        return _dataArray.get(index, _data);
    }
    
    @Override
    public void run() {
        int length = _dataArray.length();
        long prevTime = System.nanoTime();
        long currTime = prevTime;
        
        while (_running) {
            read(_rand.nextInt(length));
            _cnt++;
            
            currTime = System.nanoTime();
            _latStats.countLatency((int)(currTime - prevTime)/1000);
            prevTime = currTime;
        }
    }
}
