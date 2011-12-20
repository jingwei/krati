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

package test.io;

import java.io.File;


import krati.io.ChannelReader;
import krati.io.ChannelWriter;
import krati.io.DataReader;
import krati.io.DataWriter;

/**
 * TestChannelReaderChannelWriter
 * 
 * @author jwu
 * 
 */
public class TestChannelReaderChannelWriter extends AbstractTestDataRW {
    
    @Override
    protected DataReader createDataReader(File file) {
        return new ChannelReader(file);
    }
    
    @Override
    protected DataWriter createDataWriter(File file) {
        return new ChannelWriter(file);
    }
}
