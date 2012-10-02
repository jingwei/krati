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

package test.io.serialization;

import java.util.HashMap;

import krati.io.Serializer;
import krati.io.serializer.JavaSerializer;

/**
 * TestJavaSerializer
 * 
 * @author jwu
 * 07/18, 2011
 * 
 */
public class TestJavaSerializer extends AbstractTestSerializer<HashMap<String, Object>> {
    
    @Override
    protected HashMap<String, Object> createObject() {
        HashMap<String,Object> userData = new HashMap<String,Object>();
        HashMap<String,String> nameStruct = new HashMap<String,String>();
        nameStruct.put("first", "Joe");
        nameStruct.put("last", "Sixpack");
        userData.put("name", nameStruct);
        userData.put("gender", "MALE");
        userData.put("verified", Boolean.FALSE);
        userData.put("userImage", "Rm9vYmFyIQ==" + _rand.nextInt());
        return userData;
    }
    
    @Override
    protected Serializer<HashMap<String, Object>> createSerializer() {
        return new JavaSerializer<HashMap<String, Object>>();
    }
}
