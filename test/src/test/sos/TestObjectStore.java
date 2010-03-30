package test.sos;

import java.io.File;


import krati.cds.DataCache;
import krati.cds.impl.DataCacheImpl;
import krati.cds.impl.store.SimpleDataStore;
import krati.cds.store.DataStore;
import krati.sos.ObjectStore;
import krati.sos.SerializableObjectStore;

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
public class TestObjectStore extends AbstractTest
{
    public TestObjectStore()
    {
        super(TestObjectStore.class.getName());
    }
    
    private DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception {
        int idStart = 0;
        int idCount = 20000;
        int segFileSizeMB = 32;
        
        DataCache cache = new DataCacheImpl(idStart,
                                            idCount,
                                            storeDir,
                                            new krati.cds.impl.segment.MemorySegmentFactory(),
                                            segFileSizeMB);
        
        return new SimpleDataStore(cache);
    }
    
    public void testObjectStore() throws Exception
    {
        cleanTestOutput();
        
        File objectStoreDir = new File(TEST_OUTPUT_DIR, "object_store");
        DataStore<byte[], byte[]> dataStore = getDataStore(objectStoreDir);
        ObjectStore<String, MemberProtos.Member> memberStore =
            new SerializableObjectStore<String, MemberProtos.Member>(dataStore, new KeySerializer(), new MemberSerializer());
        
        MemberProtos.MemberBook book = MemberDataGen.generateMemberBook(10000);
        
        for(MemberProtos.Member m : book.getMemberList()) 
        {
            memberStore.put(m.getEmail(0), m);
        }
        
        for(MemberProtos.Member m : book.getMemberList()) 
        {
            memberStore.put(m.getEmail(0), m);
        }
        
        for(MemberProtos.Member m : book.getMemberList()) 
        {
            memberStore.put(m.getEmail(0), m);
        }
        
        memberStore.persist();
        
        for(MemberProtos.Member m : book.getMemberList()) 
        {
            assertTrue("Member " + m.getMemberId(), memberStore.get(m.getEmail(0)).equals(m));
        }
        
        cleanTestOutput();
    }
}
