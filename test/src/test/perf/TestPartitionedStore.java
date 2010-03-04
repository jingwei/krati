package test.perf;

import java.io.File;

import krati.mds.impl.store.PartitionedDataStore;
import krati.mds.store.DataStore;

public class TestPartitionedStore extends TestMDSStore
{
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File mdsStoreDir) throws Exception
    {
        return new PartitionedDataStore(mdsStoreDir, 5, 1000000, 200000);
    }
    
    public static void main(String[] args)
    {
        new TestPartitionedStore().run(4, 2);
        System.out.println("done");
    }
}
