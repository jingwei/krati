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

package test.store.segment;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import test.util.FileUtils;

import junit.framework.TestCase;
import krati.core.segment.SegmentIndexBuffer;
import krati.core.segment.SegmentIndexBufferException;
import krati.core.segment.SegmentIndexBufferFileIO;
import krati.core.segment.SegmentIndexBufferIO;

/**
 * TestSegmentIndexBufferIO
 * 
 * @author jwu
 * @since 08/26, 2012
 */
public class TestSegmentIndexBufferIO extends TestCase {
    protected Random _rand = new Random();
    protected SegmentIndexBuffer _sib;
    protected SegmentIndexBufferIO _sibIO;
    
    protected SegmentIndexBuffer create() {
        return new SegmentIndexBuffer();
    }
    
    protected SegmentIndexBufferIO createIO() {
        return new SegmentIndexBufferFileIO();
    }
    
    @Override
    protected void setUp() {
        _sib = create();
        _sibIO = createIO();
    }
    
    @Override
    protected void tearDown() {
        File testDir = FileUtils.getTestDir(getClass().getSimpleName());
        try {
            FileUtils.deleteDirectory(testDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    protected void runApi(int size) throws IOException, SegmentIndexBufferException {
        int index = 0;
        int offset = 0;
        
        _sib.clear();
        assertEquals(0, _sib.size());
        
        for(int i = 0; i < size; i++) {
            _sib.add(index++, offset++);
            assertEquals(index, _sib.size());
        }
        
        assertEquals(size, _sib.size());
        
        int segId = 19;
        long lastForcedTime = System.currentTimeMillis();
        _sib.setSegmentId(segId);
        _sib.setSegmentLastForcedTime(lastForcedTime);
        assertEquals(segId, _sib.getSegmentId());
        assertEquals(lastForcedTime, _sib.getSegmentLastForcedTime());
        
        File sibFile = new File(FileUtils.getTestDir(getClass().getSimpleName()), segId + ".sib");
        int writeLength = _sibIO.write(_sib, sibFile);
        
        // Read without validating lastForcedTime
        SegmentIndexBuffer sibRead = new SegmentIndexBuffer();
        int readLength = _sibIO.read(sibRead, sibFile);
        
        assertEquals(writeLength, readLength);
        assertEquals(_sib.getSegmentId(), sibRead.getSegmentId());
        assertEquals(_sib.getSegmentLastForcedTime(), sibRead.getSegmentLastForcedTime());
        assertEquals(_sib.size(), sibRead.size());
        
        for(int i = 0; i < size; i++) {
            SegmentIndexBuffer.IndexOffset o1 = _sib.get(i);
            SegmentIndexBuffer.IndexOffset o2 = sibRead.get(i);
            assertEquals(o1.getIndex(), o2.getIndex());
            assertEquals(o1.getOffset(), o2.getOffset());
        }

        // Read with validating lastForcedTime
        sibRead = new SegmentIndexBuffer();
        readLength = _sibIO.read(sibRead, sibFile, lastForcedTime);
        
        assertEquals(writeLength, readLength);
        assertEquals(_sib.getSegmentId(), sibRead.getSegmentId());
        assertEquals(_sib.getSegmentLastForcedTime(), sibRead.getSegmentLastForcedTime());
        assertEquals(_sib.size(), sibRead.size());
        
        for(int i = 0; i < size; i++) {
            SegmentIndexBuffer.IndexOffset o1 = _sib.get(i);
            SegmentIndexBuffer.IndexOffset o2 = sibRead.get(i);
            assertEquals(o1.getIndex(), o2.getIndex());
            assertEquals(o1.getOffset(), o2.getOffset());
        }
    }
    
    public void testApi() throws Exception {
        runApi(_rand.nextInt(100000));
        runApi(0);
    }
    
    public void testApi10Times() throws Exception {
        for(int i = 0; i < 10; i++) {
            testApi();
        }
    }
}
