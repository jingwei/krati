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

package test.hash;

import java.io.File;
import java.io.IOException;

import krati.core.segment.MemorySegmentFactory;
import krati.store.StaticDataSet;
import krati.util.HashFunction;
import test.AbstractTest;
import test.StatsLog;

public abstract class EvalHashFunction extends AbstractTest {
    protected HashCollisionStats _collisionStats = new HashCollisionStats();
    
    protected EvalHashFunction(String name) {
        super(name);
    }
    
    protected abstract HashFunction<byte[]> createHashFunction();
    
    protected StaticDataSet createDataSet(File storeDir, int capacity, HashFunction<byte[]> hashFunction) throws Exception {
        return new StaticDataSet(storeDir, capacity, 10000, 5, 32, new MemorySegmentFactory(), hashFunction);
    }
    
    private void populate(StaticDataSet store) throws IOException {
        int lineCnt = _lineSeedData.size();
        long startTime = System.currentTimeMillis();
        
        try {
            for (int i = 0; i < _keyCount; i++) {
                String s = _lineSeedData.get(i % lineCnt);
                String k = s.substring(0, 30) + i;
                store.add(k.getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        
        double rate = _keyCount/(double)elapsedTime;
        rate = Math.round(rate * 100) / 100.0;
        StatsLog.logger.info("writeCount="+ _keyCount +" rate="+ rate +" per ms");
        StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
    }
    
    private void collect(StaticDataSet store) {
        int lineCnt = _lineSeedData.size();
        long startTime = System.currentTimeMillis();
        
        try {
            for (int i = 0; i < _keyCount; i++) {
                String s = _lineSeedData.get(i % lineCnt);
                String k = s.substring(0, 30) + i;
                _collisionStats.addCollisionCount(Math.abs(store.countCollisions(k.getBytes())));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        _collisionStats.print(StatsLog.logger);
        
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
    }
    
    public void test() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        HashFunction<byte[]> hashFunction = createHashFunction();
        
        File storeHomeDir = getHomeDirectory();
        int storeCapacity = (int)(_keyCount * 1.5);
        StaticDataSet store = createDataSet(storeHomeDir, storeCapacity, hashFunction);
        
        StatsLog.logger.info(">>> populate");
        populate(store);
        store.sync();
        
        StatsLog.logger.info(">>> collect collision stats");
        collect(store);
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
