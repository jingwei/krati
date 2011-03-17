package test.sos;

import java.io.File;
import java.util.List;
import java.util.Random;

import krati.sos.ObjectArrayPartition;
import krati.sos.SerializableObjectCache;
import krati.store.ArrayStorePartition;
import krati.store.StaticArrayStorePartition;
import krati.util.Chronos;
import test.AbstractTest;
import test.StatsLog;
import test.protos.MemberDataGen;
import test.protos.MemberProtos;
import test.protos.MemberSerializer;

/**
 * TestObjectPartition
 * 
 * @author jwu
 *
 */
public class TestObjectPartition extends AbstractTest {
    
    public TestObjectPartition() {
        super(TestObjectPartition.class.getName());
    }
    
    private ArrayStorePartition getPartition(File homeDir) throws Exception {
        ArrayStorePartition partition =
            new StaticArrayStorePartition(_idStart,
                                          _idCount,
                                          homeDir,
                                          new krati.core.segment.MemorySegmentFactory(),
                                          _segFileSizeMB);
        return partition;
    }
    
    public void testObjectPartition() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        cleanTestOutput();
        
        File partitionDir = getHomeDirectory();
        ArrayStorePartition partition = getPartition(partitionDir);
        ObjectArrayPartition<MemberProtos.Member> memberPartition =
            new SerializableObjectCache<MemberProtos.Member>(partition, new MemberSerializer());
        
        int numSeedMembers = 10000; 
        MemberProtos.MemberBook book = MemberDataGen.generateMemberBook(numSeedMembers);
        
        long scn = 0;
        int cacheSize = memberPartition.getObjectIdCount();
        int objectIdStart = memberPartition.getObjectIdStart();
        int objectIdEnd = objectIdStart + cacheSize;

        Chronos timer = new Chronos();
        List<MemberProtos.Member> mList = book.getMemberList();
        
        // Sequential update
        for (int i = objectIdStart; i < objectIdEnd; i++) {
            MemberProtos.Member m = mList.get(i%numSeedMembers);
            memberPartition.set(i, m, scn++);
        }
        StatsLog.logger.info("Populate " + cacheSize + " objects in " + timer.getElapsedTime());
        
        // Persist
        memberPartition.persist();
        timer.tick();
        
        // Result validation
        for (int i = objectIdStart; i < objectIdEnd; i++) {
            MemberProtos.Member m = mList.get(i%numSeedMembers);
            assertTrue("Member " + m.getMemberId(), memberPartition.get(i).equals(m));
        }
        StatsLog.logger.info("Validate " + cacheSize + " objects in " + timer.getElapsedTime());
        
        // Random update
        Random rand = new Random();
        for (int i = objectIdStart; i < objectIdEnd; i++) {
            int objectId = objectIdStart + rand.nextInt(cacheSize);
            MemberProtos.Member m = mList.get(objectId%numSeedMembers);
            memberPartition.set(objectId, m, scn++);
        }
        StatsLog.logger.info("Populate " + cacheSize + " objects in " + timer.getElapsedTime());
        
        // Persist
        memberPartition.persist();
        timer.tick();
        
        // Result validation
        for (int i = memberPartition.getObjectIdStart(); i < cacheSize; i++) {
            MemberProtos.Member m = mList.get(i%mList.size());
            assertTrue("Member " + m.getMemberId(), memberPartition.get(i).equals(m));
        }
        StatsLog.logger.info("Validate " + cacheSize + " objects in " + timer.getElapsedTime());
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
