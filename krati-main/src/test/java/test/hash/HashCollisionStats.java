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

package test.hash;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import org.apache.log4j.Logger;

public class HashCollisionStats {
    long _largeCollisionCount = 0;
    final long[] _collisionCountArray = new long[20];
    
    public HashCollisionStats() {
        Arrays.fill(_collisionCountArray, 0);
    }
    
    public void addCollisionCount(int collisionCount) {
        if(collisionCount < _collisionCountArray.length)
            _collisionCountArray[collisionCount] += 1;
        else
            _largeCollisionCount++;
    }
    
    private long getTotal() {
        long cnt = 0;
        for (int i = 0; i < _collisionCountArray.length; i++) {
            cnt += _collisionCountArray[i];
        }
        
        cnt += _largeCollisionCount;
        return cnt;
    }
    
    public void print(PrintStream out) {
        for (int i = 0; i < _collisionCountArray.length; i++) {
            out.printf("%2d,%12d%n", i, _collisionCountArray[i]);
        }
        
        if (_largeCollisionCount > 0) {
            out.printf("++,%12d%n", _largeCollisionCount);
        }
        
        out.printf("Sum%12d%n", getTotal());
    }
    
    public void print(Logger log) {
        ByteArrayOutputStream bos;
        PrintStream out;
        
        for (int i = 0; i < _collisionCountArray.length; i++) {
            bos = new ByteArrayOutputStream(15);
            out = new PrintStream(bos);
            
            out.printf("%2d,%12d", i, _collisionCountArray[i]);
            log.info(bos.toString());
            
            out.close();
        }
        
        if (_largeCollisionCount > 0) {
            bos = new ByteArrayOutputStream(15);
            out = new PrintStream(bos);
            
            out.printf("++,%12d", _largeCollisionCount);
            log.info(bos.toString());
            
            out.close();
        }
        
        // Print total
        bos = new ByteArrayOutputStream(15);
        out = new PrintStream(bos);
        
        out.printf("Sum%12d", getTotal());
        log.info(bos.toString());
        
        out.close();
    }
}
