package test.io;

import java.io.File;


import krati.io.ChannelWriter;
import krati.io.DataReader;
import krati.io.DataWriter;
import krati.io.MappedReader;

/**
 * TestMappedReaderChannelWriter
 * 
 * @author jwu
 * 
 */
public class TestMappedReaderChannelWriter extends AbstractTestDataRW {
    
    @Override
    protected DataReader createDataReader(File file) {
        return new MappedReader(file);
    }
    
    @Override
    protected DataWriter createDataWriter(File file) {
        return new ChannelWriter(file);
    }
}
