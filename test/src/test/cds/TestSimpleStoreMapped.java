package test.cds;

import java.io.File;

import krati.cds.DataCache;
import krati.cds.impl.DataCacheImpl;
import krati.cds.impl.store.SimpleDataStore;
import krati.cds.store.DataStore;

/**
 * TestSimpleStore using MappedSegment.
 * 
 * @author jwu
 *
 */
public class TestSimpleStoreMapped extends TestSimpleStore
{
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception {
        DataCache cache = new DataCacheImpl(idStart,
                                            idCount,
                                            storeDir,
                                            new krati.cds.impl.segment.MappedSegmentFactory(),
                                            segFileSizeMB);
        
        return new SimpleDataStore(cache);
    }
    
    @Override
    public void testSimpleStore() throws Exception
    {
        new TestSimpleStoreMapped().run(4, 1);
        System.out.println("done");
        cleanTestOutput();
    }
}
