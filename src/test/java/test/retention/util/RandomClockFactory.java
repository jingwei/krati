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

package test.retention.util;

import java.util.Random;

import krati.retention.clock.Clock;

/**
 * RandomClockFactory
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/11, 2011 - Created
 */
public class RandomClockFactory {
    private final Random _rand;
    private final long[] _scnArray;
    
    public RandomClockFactory(int numSources) {
        _rand = new Random();
        _scnArray = new long[numSources];
        for(int i = 0; i < numSources; i++) {
            _scnArray[i] = System.currentTimeMillis();
        }
    }
    
    public Clock next() {
        for(int i = 0; i < _scnArray.length; i++) {
            _scnArray[i] += _rand.nextInt(10) + 1;
        }
        return new Clock((long[])_scnArray.clone());
    }
}
