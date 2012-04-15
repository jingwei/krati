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

import krati.util.HashFunction;
import krati.util.MurmurHashFunction;

public class TestMurmurTieredHashFunction extends EvalTieredHashFunction {
    public TestMurmurTieredHashFunction() {
        super(TestMurmurTieredHashFunction.class.getSimpleName());
    }

    @Override
    protected HashFunction<byte[]> createHashFunction() {
        return new MurmurHashFunction();
    }
}
