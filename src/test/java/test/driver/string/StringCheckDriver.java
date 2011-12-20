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

package test.driver.string;

import java.util.List;

import test.driver.StoreReader;

/**
 * StringCheckDriver
 * 
 * @author jwu
 * 
 * @param <S> Data Store
 */
public class StringCheckDriver<S> extends StringReadDriver<S> {
    
    public StringCheckDriver(S store, StoreReader<S, String, String> reader, List<String> lineSeedData, int keyCount) {
        super(store, reader, lineSeedData, keyCount);
    }
    
    @Override
    protected void read() {
        int i = _rand.nextInt(_keyCount);
        String s = _lineSeedData.get(i%_lineSeedCount);
        String key = s.substring(0, 30) + i;
        String value = _reader.get(_store, key);
        
        if (value != null) {
            if (!s.equals(value)) {
                System.err.printf("key=\"%s\"%n", key);
                System.err.printf("    \"%s\"%n", s);
                System.err.printf("    \"%s\"%n", value);
            }
        } else {
            System.err.printf("check found null for key=\"%s\"%n", key);
        }
        _cnt++;
    }
}
