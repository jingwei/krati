package krati.store;

/**
 * DataSetHandler
 * 
 * @author jwu
 * 
 */
public interface DataSetHandler {
    
    public int count(byte[] data);
    
    public byte[] assemble(byte[] value);
    
    public byte[] assemble(byte[] value, byte[] data);
    
    public int countCollisions(byte[] value, byte[] data);
    
    public int remove(byte[] value, byte[] data);
    
    public boolean find(byte[] value, byte[] data);
}
