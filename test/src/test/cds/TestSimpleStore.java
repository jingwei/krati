package test.cds;

import java.io.File;
import java.io.IOException;

import krati.cds.impl.array.AddressArray;
import krati.cds.impl.array.SimpleDataArray;
import krati.cds.impl.array.basic.RecoverableLongArray;
import krati.cds.impl.segment.SegmentFactory;
import krati.cds.impl.segment.SegmentManager;
import krati.cds.impl.store.SimpleDataStore;
import krati.cds.store.DataStore;

/**
 * TestSimpleStore using MemorySegment.
 * 
 * @author jwu
 *
 */
public class TestSimpleStore extends EvalDataStore
{
    public TestSimpleStore()
    {
        super(TestSimpleStore.class.getName());
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
    
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception
    {
        SimpleDataArray array = new SimpleDataArray(getAddressArray(storeDir),
                                                    getSegmentManager(storeDir));
        
        return new SimpleDataStore(array);
    }
    
    public void testSimpleStore() throws Exception
    {
        new TestSimpleStore().run(4, 1);
        System.out.println("done");
        cleanTestOutput();
    }
}
