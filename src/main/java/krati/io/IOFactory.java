package krati.io;

import java.io.File;

/**
 * IOFactory
 * 
 * @author jwu
 * 03/17, 2011
 */
public class IOFactory {    
    
    /**
     * Creates a new DataReader to read from a file.
     * 
     * @param file - file to read.
     * @param type - I/O type.
     * @return a new DataReader instance.
     */
    public static DataReader createDataReader(File file, IOType type) {
        if(type == IOType.MAPPED) {
            if(file.length() <= Integer.MAX_VALUE) {
                return new MappedReader(file);
            } else {
                return new MultiMappedReader(file);
            }
        } else {
            return new ChannelReader(file);
        }
    }
    
    /**
     * Creates a new DataWriter to write to a file.
     * 
     * @param file - file to write.
     * @param type - I/O type.
     * @return a new DataWriter instance.
     */
    public static DataWriter createDataWriter(File file, IOType type) {
        if(type == IOType.MAPPED) {
            if(file.length() <= Integer.MAX_VALUE) {
                return new MappedWriter(file);
            } else {
                return new MultiMappedWriter(file);
            }
        } else {
            return new ChannelWriter(file);
        }
    }
}
