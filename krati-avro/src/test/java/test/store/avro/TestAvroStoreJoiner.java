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

package test.store.avro;

import krati.io.Serializer;
import krati.io.serializer.StringSerializerUtf8;

/**
 * TestAvroStoreJoiner
 * 
 * @author jwu
 * @since 09/26, 2011
 */
public class TestAvroStoreJoiner extends AbstractTestAvroStoreJoiner<String> {
    
    @Override
    protected String createKey(int memberId) {
        return "member." + memberId;
    }
    
    @Override
    protected Serializer<String> createKeySerializer() {
        return new StringSerializerUtf8();
    }
}
