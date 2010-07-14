package test.sos;

import java.io.File;

import krati.cds.impl.segment.SegmentFactory;
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
    
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.MemorySegmentFactory();
    }
    
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception
    {
        return new SimpleDataStore(storeDir,
                                   _idCount,   /* capacity */
                                   10000,     /* entrySize */
                                   5,         /* maxEntries */
                                   _segFileSizeMB,
                                   getSegmentFactory());
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
