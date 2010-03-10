package test.perf;

import java.io.File;

import krati.cds.DataCache;
import krati.cds.impl.DataCacheImpl;
import krati.cds.impl.store.SimpleDataStore;
import krati.cds.store.DataStore;

public class EvalSimpleStore extends EvalDataStore
{
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception {
        int idStart = 0;
        int idCount = 500000;
        int segFileSizeMB = 256;
        
        DataCache cache = new DataCacheImpl(idStart,
                                            idCount,
                                            storeDir,
                                            new krati.cds.impl.segment.MemorySegmentFactory(),
                                            segFileSizeMB);
        
        return new SimpleDataStore(cache);
    }
    
    public static void main(String[] args)
    {
        new EvalSimpleStore().run(4, 1);
        System.out.println("done");
    }
}
