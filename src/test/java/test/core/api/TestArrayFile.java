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
