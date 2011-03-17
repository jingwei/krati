package test.sos;

import java.io.File;
import java.util.Iterator;

import krati.core.segment.SegmentFactory;
import krati.sos.ObjectStore;
import krati.sos.SerializableObjectStore;
import krati.store.DataStore;
import krati.store.StaticDataStore;

import test.AbstractTest;

import test.protos.KeySerializer;
import test.protos.MemberDataGen;
import test.protos.MemberProtos;
import test.protos.MemberSerializer;

/**
 * Test SerializableObjectStore
 * 
 * @author jwu
 *
 */
public class TestObjectStore extends AbstractTest {
    
    public TestObjectStore() {
        super(TestObjectStore.class.getName());
    }
    
    protected SegmentFactory getSegmentFactory() {
        return new krati.core.segment.MemorySegmentFactory();
    }
    
    protected DataStore<byte[], byte[]> getDataStore(File storeDir, int capacity) throws Exception {
        return new StaticDataStore(storeDir,
                                   capacity,
                                   1000,      /* entrySize */
                                   5,         /* maxEntries */
                                   _segFileSizeMB,
                                   getSegmentFactory());
    }
    
    public void testObjectStore() throws Exception {
        cleanTestOutput();
        
        int memberCnt = 10000;
        int capacity = memberCnt * 2;
        
        File objectStoreDir = new File(TEST_OUTPUT_DIR, "object_store");
        DataStore<byte[], byte[]> dataStore = getDataStore(objectStoreDir, capacity);
        ObjectStore<String, MemberProtos.Member> memberStore =
            new SerializableObjectStore<String, MemberProtos.Member>(dataStore, new KeySerializer(), new MemberSerializer());
        
        MemberProtos.MemberBook book = MemberDataGen.generateMemberBook(memberCnt);
        
        for (MemberProtos.Member m : book.getMemberList()) {
            memberStore.put(m.getEmail(0), m);
        }
        
        for (MemberProtos.Member m : book.getMemberList()) {
            memberStore.put(m.getEmail(0), m);
        }
        
        for (MemberProtos.Member m : book.getMemberList()) {
            memberStore.put(m.getEmail(0), m);
        }
        
        memberStore.persist();
        
        for (MemberProtos.Member m : book.getMemberList()) {
            assertTrue("Member " + m.getMemberId(), memberStore.get(m.getEmail(0)).equals(m));
        }
        
        Iterator<String> keyIter = memberStore.keyIterator();
        while (keyIter.hasNext()) {
            String key = keyIter.next();
            MemberProtos.Member m = memberStore.get(key);
            assertTrue("Member " + m.getMemberId() + ": key=" + key + " email=" + m.getEmail(0), m.getEmail(0).equals(key));
        }
        
        cleanTestOutput();
    }
}
