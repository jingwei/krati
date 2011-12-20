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

import krati.store.DataSet;

/**
 * DataSetWriter
 * 
 * @author jwu
 * 
 */
public class DataSetWriter extends DataSetRunner {
    
    public DataSetWriter(DataSet<byte[]> store, List<String> seedData, int keyCount) {
        super(store, seedData, keyCount);
    }
    
    @Override
    protected void op() {
        try {
            int i = _rand.nextInt(_keyCount);
            String s = _lineSeedData.get(i%_lineSeedCount);
            String k = s.substring(0, 30) + i;
            _store.add(k.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
