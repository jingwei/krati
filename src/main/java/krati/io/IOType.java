package krati.io;

/**
 * IOType
 * 
 * @author jwu
 * 03/17, 2011
 */
public enum IOType {
    /**
     * I/O operations via Java NIO memory map. 
     */
    MAPPED,
    
    /**
     * I/O operations vial Java NIO file channel.
     */
    CHANNEL;
}
