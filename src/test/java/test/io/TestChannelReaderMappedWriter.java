package test.io;

import java.io.File;


import krati.io.ChannelReader;
import krati.io.DataReader;
import krati.io.DataWriter;
import krati.io.MappedWriter;

/**
 * TestChannelReaderMappedWriter
 * 
 * @author jwu
 * 
 */
public class TestChannelReaderMappedWriter extends AbstractTestDataRW {
    
    @Override
    protected DataReader createDataReader(File file) {
        return new ChannelReader(file);
    }
    
    @Override
    protected DataWriter createDataWriter(File file) {
        return new MappedWriter(file);
    }
}
