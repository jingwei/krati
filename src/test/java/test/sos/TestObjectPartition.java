package test.sos;

import java.io.File;
import java.util.List;
import java.util.Random;

import krati.sos.ObjectPartition;
import krati.sos.SerializableObjectPartition;
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
        ObjectPartition<MemberProtos.Member> memberPartition =
            new SerializableObjectPartition<MemberProtos.Member>(partition, new MemberSerializer());
        
        int numSeedMembers = 10000; 
        MemberProtos.MemberBook book = MemberDataGen.generateMemberBook(numSeedMembers);
        
        long scn = 0;
        int size = memberPartition.getObjectIdCount();
        int objectIdStart = memberPartition.getObjectIdStart();
        int objectIdEnd = objectIdStart + size;

        Chronos timer = new Chronos();
        List<MemberProtos.Member> mList = book.getMemberList();
        
        // Sequential update
        for (int i = objectIdStart; i < objectIdEnd; i++) {
            MemberProtos.Member m = mList.get(i%numSeedMembers);
            memberPartition.set(i, m, scn++);
        }
        StatsLog.logger.info("Populate " + size + " objects in " + timer.getElapsedTime());
        
        // Persist
        memberPartition.persist();
        timer.tick();
        
        // Result validation
        for (int i = objectIdStart; i < objectIdEnd; i++) {
            MemberProtos.Member m = mList.get(i%numSeedMembers);
            assertTrue("Member " + m.getMemberId(), memberPartition.get(i).equals(m));
        }
        StatsLog.logger.info("Validate " + size + " objects in " + timer.getElapsedTime());
        
        // Random update
        Random rand = new Random();
        for (int i = objectIdStart; i < objectIdEnd; i++) {
            int objectId = objectIdStart + rand.nextInt(size);
            MemberProtos.Member m = mList.get(objectId%numSeedMembers);
            memberPartition.set(objectId, m, scn++);
        }
        StatsLog.logger.info("Populate " + size + " objects in " + timer.getElapsedTime());
        
        // Persist
        memberPartition.persist();
        timer.tick();
        
        // Result validation
        for (int i = memberPartition.getObjectIdStart(); i < size; i++) {
            MemberProtos.Member m = mList.get(i%mList.size());
            assertTrue("Member " + m.getMemberId(), memberPartition.get(i).equals(m));
        }
        StatsLog.logger.info("Validate " + size + " objects in " + timer.getElapsedTime());
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
