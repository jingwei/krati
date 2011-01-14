package krati.store;

import java.io.IOException;

import krati.Persistable;

/**
 * ArrayStore
 * 
 * @author jwu
 * 01/10, 2011
 *
 */
public interface ArrayStore extends Persistable {
    
    public int capacity();
    
    public byte[] get(int memberId);
    
    public int get(int memberId, byte[] dst);
    
    public int get(int memberId, byte[] dst, int offset);
    
    public void set(int memberId, byte[] data, long scn) throws Exception;
    
    public void set(int memberId, byte[] data, int offset, int length, long scn) throws Exception;
    
    public void delete(int memberId, long scn) throws Exception;
        
    public void clear() throws IOException;
}
