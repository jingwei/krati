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

import krati.array.DataArray;

/**
 * DataArrayChecker
 * 
 * @author jwu
 * 
 */
public class DataArrayChecker extends DataArrayReader {
    
    public DataArrayChecker(DataArray dataArray, List<String> seedData) {
        super(dataArray, seedData);
    }
    
    void check(int index) {
        String line = _lineSeedData.get(index % _lineSeedData.size());
        
        byte[] b = _dataArray.get(index);
        if (b != null) {
            String s = new String(b);
            if (!s.equals(line)) {
                throw new RuntimeException("[" + index + "]=" + s + " expected=" + line);
            }
        } else {
            if (line != null) {
                throw new RuntimeException("[" + index + "]=null expected=" + line);
            }
        }
    }
    
    @Override
    public void run() {
        int length = _dataArray.length();
        while (_running) {
            int index = _rand.nextInt(length);
            check(index);
            _cnt++;
        }
    }
}
