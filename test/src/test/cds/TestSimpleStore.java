package test.cds;

import java.io.File;

import krati.cds.DataCache;
import krati.cds.impl.DataCacheImpl;
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
    
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception {
        DataCache cache = new DataCacheImpl(idStart,
                                            idCount,
                                            storeDir,
                                            new krati.cds.impl.segment.MemorySegmentFactory(),
                                            segFileSizeMB);
        
        return new SimpleDataStore(cache);
    }
    
    public void testSimpleStore() throws Exception
    {
        new TestSimpleStore().run(4, 1);
        System.out.println("done");
        cleanTestOutput();
    }
}
