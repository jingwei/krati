package test.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Random;


import krati.io.ChannelWriter;
import krati.io.DataReader;
import krati.io.DataWriter;
import krati.io.MultiMappedReader;

/**
 * TestMultiMappedReaderChannelWriter
 * 
 * @author jwu
 * 02/26, 2011
 */
public class TestMultiMappedReaderChannelWriter extends AbstractTestDataRW {
    
    @Override
    protected DataReader createDataReader(File file) {
        return new MultiMappedReader(file);
    }
    
    @Override
    protected DataWriter createDataWriter(File file) {
        return new ChannelWriter(file);
    }
    
    public void testLargeFile() throws IOException {
        Random rand = new Random();
        int length1 = Integer.MAX_VALUE;
        int length2 = rand.nextInt(Integer.MAX_VALUE);
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        
        long fileLength = 0L;
        fileLength += length1;
        fileLength += length2;
        raf.setLength(fileLength);
        
        int num = (int)(fileLength >> MultiMappedReader.BUFFER_BITS);
        num += (fileLength & MultiMappedReader.BUFFER_MASK) > 0 ? 1 : 0;
        ArrayList<Long> posList = new ArrayList<Long>(num);
        ArrayList<Integer> valList = new ArrayList<Integer>(num);
        
        long position = 0L;
        for(int i = 0; i < num; i++) {
            position += rand.nextInt(MultiMappedReader.BUFFER_SIZE);
            position = Math.min(position, fileLength - 4);
            position -= position % 4;
            posList.add(position);
            valList.add(rand.nextInt());
            position = i * (long)MultiMappedReader.BUFFER_SIZE;
        }
        
        DataWriter writer = createDataWriter(file);
        writer.open();
        for(int i = 0, cnt = posList.size(); i < cnt; i++) {
            writer.writeInt(posList.get(i), valList.get(i));
        }
        writer.flush();
        writer.close();
        
        DataReader reader = createDataReader(file);
        reader.open();
        for(int i = 0, cnt = posList.size(); i < cnt; i++) {
            position = posList.get(i);
            int valW = valList.get(i);
            int valR = reader.readInt(position);
            assertEquals(valW, valR);
        }
        reader.close();
    }
}
