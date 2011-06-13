package krati.examples;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import krati.core.segment.SegmentFactory;
import krati.store.ArrayStorePartition;
import krati.store.StaticArrayStorePartition;

/**
 * Sample code for Krati Partition.
 * 
 * @author jwu
 * 
 */
public class KratiDataPartition {
    private final ArrayStorePartition _partition;
    
    /**
     * Constructs KratiDataPartition.
     * 
     * @param idStart    the start of member IDs.
     * @param idCount    the count of member IDs.
     * @param homeDir    the home directory for storing data.
     * @throws Exception if a partition instance can not be created.
     */
    public KratiDataPartition(int idStart, int idCount, File homeDir) throws Exception {
        _partition = new StaticArrayStorePartition(idStart,
                                                   idCount,
                                                   homeDir,
                                                   createSegmentFactory(),
                                                   128);
    }
    
    /**
     * @return the underlying data partition.
     */
    public final ArrayStorePartition getPartition() {
        return _partition;
    }
    
    /**
     * Creates a segment factory.
     * Subclasses can override this method to provide a specific segment factory
     * such as ChannelSegmentFactory and MappedSegmentFactory.
     * 
     * @return the segment factory. 
     */
    protected SegmentFactory createSegmentFactory() {
        return new krati.core.segment.MemorySegmentFactory();
    }
    
    /**
     * Creates data for a given member.
     * Subclasses can override this method to provide specific data for a given member.
     * 
     * @return
     */
    protected byte[] createDataForMember(int memberId) {
        return ("Here is your data for member " + memberId).getBytes();
    }
    
    /**
     * Populates the underlying data partition.
     * 
     * @throws Exception
     */
    public void populate() throws Exception {
        for (int i = 0, cnt = _partition.getIdCount(); i < cnt; i++) {
            int memberId = _partition.getIdStart() + i;
            _partition.set(memberId, createDataForMember(memberId), System.nanoTime());
        }
        _partition.sync();
    }
    
    /**
     * Perform a number of random reads from the underlying data partition.
     * 
     * @param readCnt the number of reads
     */
    public void doRandomReads(int readCnt) {
        Random rand = new Random();
        int idStart = _partition.getIdStart();
        int idCount = _partition.getIdCount();
        for (int i = 0; i < readCnt; i++) {
            int memberId = idStart + rand.nextInt(idCount);
            System.out.printf("MemberId=%-10d MemberData=%s%n", memberId, new String(_partition.get(memberId)));
        }
    }
    
    /**
     * Closes the underlying partition.
     * 
     * @throws IOException
     */
    public void close() throws IOException {
        _partition.close();
    }
    
    /**
     * java -Xmx4G krati.examples.KratiDataPartition idStart idCount homeDir
     */
    public static void main(String[] args) {
        try {
            // Parse arguments: idStart idCount homeDir
            int idStart = Integer.parseInt(args[0]);
            int idCount = Integer.parseInt(args[1]);
            File homeDir = new File(args[2]);
            
            // Create an instance of KratiDataPartition
            File dcHomeDir = new File(homeDir, KratiDataPartition.class.getSimpleName());
            KratiDataPartition dc = new KratiDataPartition(idStart, idCount, dcHomeDir);
            
            // Populate data partition
            dc.populate();
            
            // Perform some random reads from data partition.
            dc.doRandomReads(10);
            
            // Close data partition
            dc.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
