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

package test.core.api;

import java.io.File;
import java.io.IOException;

import test.util.FileUtils;

import junit.framework.TestCase;
import krati.core.array.basic.ArrayFile;

/**
 * TestArrayFile
 * 
 * @author jwu
 * 06/07, 2011
 * 
 */
public class TestArrayFile extends TestCase {
    protected File _homeDir;
    protected ArrayFile _arrayFile;
    
    @Override
    protected void setUp() {
        try {
            _homeDir = FileUtils.getTestDir(getClass().getSimpleName());
            _arrayFile = createArrayFile(_homeDir);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            // FileUtils.deleteDirectory(_homeDir);
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            _homeDir = null;
            _arrayFile = null;
        }
    }
    
    protected ArrayFile createArrayFile(File homeDir) throws IOException {
        File file = new File(homeDir, "indexes.dat");
        return new ArrayFile(file, 1 << 16, 8);
    }
    
    public void testArrayLength() throws IOException {
        int length = 0;
        
        length = 1 << 26;
        _arrayFile.setArrayLength(length, null);
        assertEquals(length, _arrayFile.getArrayLength());
        
        length = 1 << 27;
        _arrayFile.setArrayLength(length, null);
        assertEquals(length, _arrayFile.getArrayLength());
        
        length = 1 << 28;
        _arrayFile.setArrayLength(length, null);
        assertEquals(length, _arrayFile.getArrayLength());
        
        length = 1 << 29;
        _arrayFile.setArrayLength(length, null);
        assertEquals(length, _arrayFile.getArrayLength());
        
        length = 1 << 30;
        _arrayFile.setArrayLength(length, null);
        assertEquals(length, _arrayFile.getArrayLength());
        
        length = Integer.MAX_VALUE;
        _arrayFile.setArrayLength(length, null);
        assertEquals(length, _arrayFile.getArrayLength());
    }
}
