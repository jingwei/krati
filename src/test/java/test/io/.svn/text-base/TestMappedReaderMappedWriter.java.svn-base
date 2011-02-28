package test.io;

import java.io.File;


import krati.io.DataReader;
import krati.io.DataWriter;
import krati.io.MappedReader;
import krati.io.MappedWriter;

/**
 * TestMappedReaderMappedWriter
 * 
 * @author jwu
 * 
 */
public class TestMappedReaderMappedWriter extends AbstractTestDataRW {
    
    @Override
    protected DataReader createDataReader(File file) {
        return new MappedReader(file);
    }
    
    @Override
    protected DataWriter createDataWriter(File file) {
        return new MappedWriter(file);
    }
}
