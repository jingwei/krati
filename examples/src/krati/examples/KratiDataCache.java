package krati.examples;

import java.io.File;
import java.util.Random;

import krati.core.segment.SegmentFactory;
import krati.store.ArrayStorePartition;
import krati.store.StaticArrayStorePartition;

/**
 * Sample code for Krati DataCache.
 * 
 * @author jwu
 *
 */
public class KratiDataCache
{
    private final ArrayStorePartition _cache;
    
    /**
     * Constructs KratiDataCache.
     * 
     * @param idStart    the start of member IDs.
     * @param idCount    the count of member IDs.
     * @param homeDir    the home directory for storing data.
     * @throws Exception if a DataCache instance can not be created.
     */
    public KratiDataCache(int idStart, int idCount, File homeDir) throws Exception
    {
        _cache = new StaticArrayStorePartition(idStart,
                                   idCount,
                                   homeDir,
                                   createSegmentFactory(),
                                   128);
    }
    
    /**
     * @return the underlying data cache.
     */
    public final ArrayStorePartition getDataCache()
    {
        return _cache;
    }
    
    /**
     * Creates a segment factory.
     * Subclasses can override this method to provide a specific segment factory
     * such as ChannelSegmentFactory and MappedSegmentFactory.
     * 
     * @return the segment factory. 
     */
    protected SegmentFactory createSegmentFactory()
    {
        return new krati.core.segment.MemorySegmentFactory();
    }
    
    /**
     * Creates data for a given member.
     * Subclasses can override this method to provide specific data for a given member.
     * 
     * @return
     */
    protected byte[] createDataForMember(int memberId)
    {
        return ("Here is your data for member " + memberId).getBytes();
    }
    
    /**
     * Populates the underlying data cache.
     * 
     * @throws Exception
     */
    public void populate() throws Exception
    {
        for(int i = 0, cnt = _cache.getIdCount(); i < cnt; i++)
        {
            int memberId = _cache.getIdStart() + i;
            _cache.set(memberId, createDataForMember(i), System.nanoTime());
        }
        _cache.sync();
    }
    
    /**
     * Perform a number of random reads from the underlying data cache.
     * 
     * @param readCnt the number of reads
     */
    public void doRandomReads(int readCnt)
    {
        Random rand = new Random();
        int idStart = _cache.getIdStart();
        int idCount = _cache.getIdCount();
        for(int i = 0; i < readCnt; i++)
        {
            int memberId = idStart + rand.nextInt(idCount);
            System.out.printf("MemberId=%-10d MemberData=%s%n", memberId, new String(_cache.get(memberId)));
        }
    }
    
    /**
     * java -Xmx4G krati.examples.KratiDataCache idStart idCount homeDir
     * 
     * @param args
     */
    public static void main(String[] args)
    {
        try
        {
            // Parse arguments: idStart idCount homeDir
            int idStart = Integer.parseInt(args[0]);
            int idCount = Integer.parseInt(args[1]);
            File homeDir = new File(args[2]);
            
            // Create an instance of Krati DataCache
            File dcHomeDir = new File(homeDir, KratiDataCache.class.getSimpleName());
            KratiDataCache dc = new KratiDataCache(idStart, idCount, dcHomeDir);
            
            // Populate data cache
            dc.populate();
            
            // Perform some random reads from data cache.
            dc.doRandomReads(10);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}
