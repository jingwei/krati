package test.cds;

import java.io.File;

import krati.cds.DataCache;
import krati.cds.impl.DataCacheImpl;
import krati.cds.impl.store.SimpleDataStore;
import krati.cds.store.DataStore;

/**
 * TestSimpleStore using ChannelSegment.
 * 
 * @author jwu
 *
 */
public class TestSimpleStoreChannel extends TestSimpleStore
{
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception {
        DataCache cache = new DataCacheImpl(idStart,
                                            idCount,
                                            storeDir,
                                            new krati.cds.impl.segment.ChannelSegmentFactory(),
                                            segFileSizeMB);
        
        return new SimpleDataStore(cache);
    }
    
    @Override
    public void testSimpleStore() throws Exception
    {
        new TestSimpleStoreChannel().run(4, 1);
        System.out.println("done");
        cleanTestOutput();
    }
}
