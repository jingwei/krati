package krati.cds;

import java.io.IOException;

/**
 * DataCache
 * 
 * @author jwu
 *
 */
public interface DataCache extends Persistable
{
    public int getIdCount();
    
    public int getIdStart();
    
    public byte[] getData(int memberId);
    
    public int getData(int memberId, byte[] dst);
    
    public int getData(int memberId, byte[] dst, int offset);
    
    public void setData(int memberId, byte[] data, long scn) throws Exception;

    public void setData(int memberId, byte[] data, int offset, int length, long scn) throws Exception;

    public void deleteData(int memberId, long scn) throws Exception;
    
    public void clear() throws IOException;
}
