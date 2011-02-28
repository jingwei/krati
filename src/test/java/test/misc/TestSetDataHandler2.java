package test.misc;

import krati.store.DataSetHandler;
import test.AbstractTest;

/**
 * TestSetDataHandler2
 * 
 * @author jwu
 * 
 */
public class TestSetDataHandler2 extends AbstractTest {
    DataSetHandler _dataHandler = new DataSetHandler2();
    static String str1 = "0123456789A0123456789B0123456789C";
    static String str2 = "0123456789D0123456789E0123456789F";
    static String str3 = "0123456789G0123456789H0123456789I";
    
    public TestSetDataHandler2() {
        super(TestSetDataHandler2.class.getSimpleName());
    }
    
    public void test() {
        int valCnt;
        
        byte[] value1 = str1.getBytes();
        byte[] value2 = str2.getBytes();
        byte[] value3 = str3.getBytes();
        
        // Add value1
        byte[] data1 = _dataHandler.assemble(value1);
        valCnt = _dataHandler.count(data1);
        assertEquals("Failed on assemble(byte[] value) valueCount: ", 1, valCnt);
        assertTrue("Failed on assemble(byte[] value)", _dataHandler.find(value1, data1));
        
        assertEquals("Failed on countCollisions(byte[] value, byte[] data) collisions: ",
                1, _dataHandler.countCollisions(value1, data1));
        assertEquals("Failed on countCollisions(byte[] value, byte[] data) collisions: ",
                -1, _dataHandler.countCollisions(value2, data1));
        
        // Add value2
        byte[] data2 = _dataHandler.assemble(value2, data1);
        valCnt = _dataHandler.count(data2);
        assertEquals("Failed on assemble(byte[] value, byte[] data) valueCount: ", 2, valCnt);
        assertTrue("Failed on assemble(byte[] value, byte[] data)", _dataHandler.find(value1, data2));
        assertTrue("Failed on assemble(byte[] value, byte[] data)", _dataHandler.find(value2, data2));
        
        assertEquals("Failed on countCollisions(byte[] value, byte[] data) collisions: ",
                2, _dataHandler.countCollisions(value1, data2));
        assertEquals("Failed on countCollisions(byte[] value, byte[] data) collisions: ",
                2, _dataHandler.countCollisions(value2, data2));
        assertEquals("Failed on countCollisions(byte[] value, byte[] data) collisions: ",
                -2, _dataHandler.countCollisions(value3, data2));
        
        // Add value3
        byte[] data3 = _dataHandler.assemble(value3, data2);
        valCnt = _dataHandler.count(data3);
        assertEquals("Failed on assemble(byte[] value, byte[] data) valueCount: ", 3, valCnt);
        assertTrue("Failed on assemble(byte[] value, byte[] data)", _dataHandler.find(value1, data3));
        assertTrue("Failed on assemble(byte[] value, byte[] data)", _dataHandler.find(value2, data3));
        assertTrue("Failed on assemble(byte[] value, byte[] data)", _dataHandler.find(value3, data3));
        
        assertEquals("Failed on countCollisions(byte[] value, byte[] data) collisions: ",
                3, _dataHandler.countCollisions(value1, data3));
        assertEquals("Failed on countCollisions(byte[] value, byte[] data) collisions: ",
                3, _dataHandler.countCollisions(value2, data3));
        assertEquals("Failed on countCollisions(byte[] value, byte[] data) collisions: ",
                3, _dataHandler.countCollisions(value3, data3));
        
        // Delete value2
        int len = _dataHandler.remove(value2, data3);
        byte[] data4 = new byte[len];
        System.arraycopy(data3, 0, data4, 0, len);
        
        valCnt = _dataHandler.count(data4);
        assertEquals("Failed on remove(byte[] value, byte[] data) valueCount: ", 2, valCnt);
        assertTrue("Failed on remove(byte[] value, byte[] data)", _dataHandler.find(value1, data4));
        assertTrue("Failed on remove(byte[] value, byte[] data)", _dataHandler.find(value3, data4));
        
        assertEquals("Failed on countCollisions(byte[] value, byte[] data) collisions: ",
                -2, _dataHandler.countCollisions(value2, data4));
        
        // Delete value1
        len = _dataHandler.remove(value1, data4);
        byte[] data5 = new byte[len];
        System.arraycopy(data4, 0, data5, 0, len);
        
        valCnt = _dataHandler.count(data5);
        assertEquals("Failed on remove(byte[] value, byte[] data) valueCount: ", 1, valCnt);
        assertTrue("Failed on remove(byte[] value, byte[] data)", _dataHandler.find(value3, data5));
        
        assertEquals("Failed on countCollisions(byte[] value, byte[] data) collisions: ",
                -1, _dataHandler.countCollisions(value1, data5));
        
        // Delete value3
        len = _dataHandler.remove(value3, data5);
        assertEquals("Failed on remove(byte[] value, byte[] data)", 0, len);
    }
}
