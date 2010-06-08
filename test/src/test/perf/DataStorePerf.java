package test.perf;

import java.io.File;

import krati.cds.impl.segment.SegmentFactory;
import krati.cds.impl.store.SimpleDataStore;
import krati.cds.store.DataStore;
import test.cds.TestSimpleStore;

public class DataStorePerf extends TestSimpleStore
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.MemorySegmentFactory();
    }
    
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception
    {
        return new SimpleDataStore(storeDir,
                                   5000000,   /* capacity */
                                   10000,     /* entrySize */
                                   5,         /* maxEntries */
                                   256,       /* segFileSizeMB */
                                   getSegmentFactory());
    }
    public static void main(String[] args)
    {
        new DataStorePerf().run(4, 1);
        System.out.println("done");
    }
}
