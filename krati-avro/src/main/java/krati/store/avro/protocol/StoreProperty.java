/*
 * Copyright (c) 2011 LinkedIn, Inc
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

package krati.store.avro.protocol;

/**
 * StoreProperty
 * 
 * @author jwu
 * @since 10/03, 2011
 */
public class StoreProperty {
    private final String _key;
    private final String _value;
    
    public StoreProperty(String key, String value) {
        if(key == null) {
            throw new NullPointerException("key");
        }
        this._key = key;
        this._value = value;
    }
    
    public String getKey() {
        return _key;
    }
    
    public String getValue() {
        return _value;
    }
    
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append(_key).append("=").append(_value);
        return b.toString();
    }
}
