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

package test.set.api;

import java.io.File;
import java.util.Random;

import junit.framework.TestCase;
import krati.store.DataSet;
import test.util.FileUtils;
import test.util.RandomBytes;

/**
 * AbstractTestDataSetApi
 * 
 * @author jwu
 * 06/06, 2011
 * 
 */
public abstract class AbstractTestDataSetApi extends TestCase {
    protected File _homeDir;
    protected DataSet<byte[]> _store;
    protected final Random _rand = new Random();
    
    @Override
    protected void setUp() {
        try {
            _homeDir = FileUtils.getTestDir(getClass().getSimpleName());
            _store = createStore(_homeDir);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            _store.close();
            FileUtils.deleteDirectory(_homeDir);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    protected abstract DataSet<byte[]> createStore(File homeDir) throws Exception;
    
    public void testApiBasics() throws Exception {
        byte[] value;
        
        // random value
        value = RandomBytes.getBytes();
        assertEquals(true, _store.isOpen());
        assertEquals(true, _store.add(value));
        assertEquals(true, _store.has(value));
        assertEquals(true, _store.has(value));
        assertEquals(true, _store.delete(value));
        assertEquals(false, _store.has(value));
        
        // random value
        value = RandomBytes.getBytes();
        assertEquals(true, _store.isOpen());
        assertEquals(true, _store.add(value));
        assertEquals(true, _store.has(value));
        _store.close();
        _store.open();
        assertEquals(true, _store.has(value));
        assertEquals(true, _store.delete(value));
        assertEquals(false, _store.has(value));
        
        _store.close();
        _store.open();
        assertEquals(false, _store.has(value));
        
        // empty value
        value = new byte[0];
        assertEquals(true, _store.isOpen());
        assertEquals(true, _store.add(value));
        assertEquals(true, _store.has(value));
        assertEquals(true, _store.delete(value));
        assertEquals(false, _store.has(value));
        
        _store.close();
        _store.open();
        assertEquals(false, _store.has(value));
        
        // null value
        assertEquals(false, _store.add(null));
        assertEquals(false, _store.has(null));
        assertEquals(false, _store.delete(null));
        
        _store.sync();
    }
    
    public void testClear() throws Exception {
        byte[] value;
        
        value = RandomBytes.getBytes();
        assertEquals(true, _store.isOpen());
        assertEquals(true, _store.add(value));
        assertEquals(true, _store.has(value));
        
        _store.close();
        _store.open();
        assertEquals(true, _store.has(value));
        
        _store.clear();
        assertEquals(false, _store.has(value));
        
        _store.close();
        _store.open();
        assertEquals(false, _store.has(value));
    }
}
