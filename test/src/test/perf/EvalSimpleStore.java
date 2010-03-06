package test.perf;

import java.io.File;

import krati.mds.MDSCache;
import krati.mds.impl.MDSCacheImpl;
import krati.mds.impl.store.SimpleDataStore;
import krati.mds.store.DataStore;

public class EvalSimpleStore extends EvalMDSStore
{
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File mdsStoreDir) throws Exception {
        int idStart = 0;
        int idCount = 500000;
        int segFileSizeMB = 256;
        
        MDSCache mds = new MDSCacheImpl(idStart,
                                        idCount,
                                        mdsStoreDir,
                                        new krati.mds.impl.segment.MemorySegmentFactory(),
                                        segFileSizeMB);
        
        return new SimpleDataStore(mds);
    }
    
    public static void main(String[] args)
    {
        new EvalSimpleStore().run(4, 1);
        System.out.println("done");
    }
}
