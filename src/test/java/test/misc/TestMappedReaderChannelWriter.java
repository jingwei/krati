package test.misc;

import java.io.File;

import krati.io.ChannelWriter;
import krati.io.DataReader;
import krati.io.DataWriter;
import krati.io.MappedReader;

public class TestMappedReaderChannelWriter extends AbstractTestDataRW {

    public TestMappedReaderChannelWriter() {
        super(TestMappedReaderChannelWriter.class.getSimpleName());
    }
    
    @Override
    protected DataReader createDataReader(File file) {
        return new MappedReader(file);
    }

    @Override
    protected DataWriter createDataWriter(File file) {
        return new ChannelWriter(file);
    }
}
