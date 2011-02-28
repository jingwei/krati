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
