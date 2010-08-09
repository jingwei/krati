package krati.cds.store;

public interface StoreDataHandler
{
    public byte[] assemble(byte[] key, byte[] value);
    
    public byte[] assemble(byte[] key, byte[] value, byte[] data);
    
    public int countCollisions(byte[] key, byte[] data);
    
    public byte[] extractByKey(byte[] key, byte[] data);
    
    public int removeByKey(byte[] key, byte[] data);    
}
