package test.misc;

import java.io.File;

import krati.io.DataReader;
import krati.io.DataWriter;
import krati.io.MappedReader;
import krati.io.MappedWriter;

public class TestMappedReaderMappedWriter extends AbstractTestDataRW {

    public TestMappedReaderMappedWriter() {
        super(TestMappedReaderMappedWriter.class.getSimpleName());
    }
    
    @Override
    protected DataReader createDataReader(File file) {
        return new MappedReader(file);
    }

    @Override
    protected DataWriter createDataWriter(File file) {
        return new MappedWriter(file);
    }
}
