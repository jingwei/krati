package test.misc;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import krati.io.DataReader;
import krati.io.DataWriter;
import test.AbstractTest;

/**
 * 
 * @author jwu
 *
 */
public abstract class AbstractTestDataRW extends AbstractTest {

    public AbstractTestDataRW(String name) {
        super(name);
    }
    
    protected abstract DataWriter createDataWriter(File file);
    protected abstract DataReader createDataReader(File file);
    
    public void testDataReadWrite() throws IOException
    {
        File dir = getHomeDirectory();
        if(!dir.exists()) dir.mkdirs();
        cleanDirectory(dir);
        
        int fileLength = 0;
        File file = new File(dir, getClass().getSimpleName() + ".dat");
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        
        fileLength = 1000;
        raf.setLength(fileLength);
        DataWriter writer = createDataWriter(file);
        writer.open();
        
        writer.writeInt(12345);
        writer.writeInt(54321);
        writer.writeInt(Integer.MAX_VALUE);
        
        writer.writeLong(0L);
        writer.writeLong(-123L);
        writer.writeLong(Long.MAX_VALUE);
        
        writer.writeShort((short)871);
        writer.writeShort((short)-23);
        writer.writeShort(Short.MAX_VALUE);
        
        DataReader reader = createDataReader(file);
        reader.open();
        
        int intValue;
        intValue = reader.readInt();
        assertEquals(12345, intValue);
        
        intValue = reader.readInt();
        assertEquals(54321, intValue);
        
        intValue = reader.readInt();
        assertEquals(Integer.MAX_VALUE, intValue);
        
        long longValue;
        longValue = reader.readLong();
        assertEquals(0L, longValue);
        
        longValue = reader.readLong();
        assertEquals(-123L, longValue);
        
        longValue = reader.readLong();
        assertEquals(Long.MAX_VALUE, longValue);
        
        short shortValue;
        shortValue = reader.readShort();
        assertEquals((short)871, shortValue);
        
        shortValue = reader.readShort();
        assertEquals((short)-23, shortValue);
        
        shortValue = reader.readShort();
        assertEquals(Short.MAX_VALUE, shortValue);
        
        writer.close();
        reader.close();
        
        cleanDirectory(dir);
    }
}
