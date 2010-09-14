package krati.store;

import java.io.IOException;

import krati.Persistable;

/**
 * DataCache - Partitioned array for storing raw member data (i.e. bytes) at specified indexes (i.e. memberId).
 * 
 * @author jwu
 *
 */
public interface DataCache extends Persistable
{
    public int getIdCount();
    
    public int getIdStart();
    
    public byte[] get(int memberId);
    
    public int get(int memberId, byte[] dst);
    
    public int get(int memberId, byte[] dst, int offset);
    
    public void set(int memberId, byte[] data, long scn) throws Exception;

    public void set(int memberId, byte[] data, int offset, int length, long scn) throws Exception;

    public void delete(int memberId, long scn) throws Exception;
    
    public void clear() throws IOException;
}
