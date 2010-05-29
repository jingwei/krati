package test.sos;

import java.io.File;
import java.io.IOException;

import krati.cds.impl.array.AddressArray;
import krati.cds.impl.array.SimpleDataArray;
import krati.cds.impl.array.basic.RecoverableLongArray;
import krati.cds.impl.segment.SegmentFactory;
import krati.cds.impl.segment.SegmentManager;
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
    
    protected AddressArray getAddressArray(File storeDir) throws Exception
    {
        return new RecoverableLongArray(idCount, 10000, 5, storeDir);
    }
    
    protected SegmentManager getSegmentManager(File storeDir) throws IOException
    {
        String segmentHome = storeDir.getCanonicalPath() + File.separator + "segs";
        return SegmentManager.getInstance(segmentHome, getSegmentFactory(), segFileSizeMB);
    }
    
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception
    {
        SimpleDataArray array = new SimpleDataArray(getAddressArray(storeDir),
                                                    getSegmentManager(storeDir));
        
        return new SimpleDataStore(array);
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
