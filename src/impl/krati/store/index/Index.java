package krati.store.index;

import java.io.IOException;

/**
 * Index.
 * 
 * @author jwu
 */
public interface Index {
    
    public byte[] lookup(byte[] keyBytes);
    
    public void update(byte[] keyBytes, byte[] metaBytes) throws Exception;
    
    public void persist() throws IOException;
    
    public void sync() throws IOException;
    
    public void clear() throws IOException;
}
