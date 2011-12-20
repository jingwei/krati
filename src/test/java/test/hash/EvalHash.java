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

import java.io.IOException;
import java.util.HashMap;

import krati.util.HashFunction;
import test.AbstractTest;
import test.StatsLog;

public abstract class EvalHash extends AbstractTest {
    
    protected EvalHash(String name) {
        super(name);
    }
    
    protected abstract HashFunction<byte[]> createHashFunction();
    
    private void populate(HashFunction<byte[]> hashFunc, HashMap<Long, Integer> hashCodes) throws IOException {
        int lineCnt = _lineSeedData.size();
        long startTime = System.currentTimeMillis();
        
        try {
            for (int i = 0; i < _keyCount; i++) {
                String s = _lineSeedData.get(i % lineCnt);
                String k = s.substring(0, 30) + i;
                long hash = hashFunc.hash(k.getBytes());
                Integer cnt = hashCodes.get(hash);
                cnt = (cnt == null) ? 0 : cnt;
                hashCodes.put(hash, cnt + 1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
    }
    
    private void collect(HashCollisionStats collisionStats, HashMap<Long, Integer> hashCodes) {
        long startTime = System.currentTimeMillis();
        
        for (Long hash : hashCodes.keySet()) {
            int cnt = hashCodes.get(hash);
            for (int i = 0; i < cnt; i++) {
                collisionStats.addCollisionCount(cnt);
            }
        }
        
        collisionStats.print(StatsLog.logger);
        
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
    }
    
    public void test() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        HashFunction<byte[]> hashFunction = createHashFunction();
        HashMap<Long, Integer> hashCodes = new HashMap<Long, Integer>();
        HashCollisionStats collisionStats = new HashCollisionStats();
        
        StatsLog.logger.info(">>> populate");
        populate(hashFunction, hashCodes);
        
        StatsLog.logger.info(">>> collect collision stats");
        collect(collisionStats, hashCodes);
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
