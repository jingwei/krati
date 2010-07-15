package test.misc;

import java.io.File;

import krati.io.ChannelReader;
import krati.io.DataReader;
import krati.io.DataWriter;
import krati.io.MappedWriter;

public class TestMappedDataWriter extends AbstractTestDataRW {

    public TestMappedDataWriter()
    {
        super(TestMappedDataWriter.class.getSimpleName());
    }
    
    @Override
    protected DataReader createDataReader(File file) {
        return new ChannelReader(file);
    }

    @Override
    protected DataWriter createDataWriter(File file) {
        return new MappedWriter(file);
    }
}
