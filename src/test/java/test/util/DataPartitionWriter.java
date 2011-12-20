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

import krati.store.ArrayStorePartition;
import test.LatencyStats;

/**
 * DataPartitionWriter
 * 
 * @author jwu
 * 
 */
public class DataPartitionWriter implements Runnable {
    ArrayStorePartition _partition;
    Random _rand = new Random();
    boolean _running = true;
    
    int _indexStart;
    int _length;
    long _cnt = 0;
    long _scn = 0;
    LatencyStats _latStats = new LatencyStats();
    final List<String> _lineSeedData;
    
    public DataPartitionWriter(ArrayStorePartition partitiion, List<String> seedData) {
        this._partition = partitiion;
        this._length = partitiion.getIdCount();
        this._indexStart = partitiion.getIdStart();
        this._scn = partitiion.getHWMark();
        this._lineSeedData = seedData;
    }
    
    public long getWriteCount() {
        return this._cnt;
    }

    public LatencyStats getLatencyStats() {
        return this._latStats;
    }
    
    public void stop() {
        _running = false;
    }
    
    void write(int index) {
        try {
            byte[] b = _lineSeedData.get(index % _lineSeedData.size()).getBytes();
            _partition.set(index, b, _scn++);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void run() {
        long prevTime = System.nanoTime();
        long currTime = prevTime;
        
        while (_running) {
            write(_indexStart + _rand.nextInt(_length));
            _cnt++;
            
            currTime = System.nanoTime();
            _latStats.countLatency((int)(currTime - prevTime)/1000);
            prevTime = currTime;
        }
    }
}
