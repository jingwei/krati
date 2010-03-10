package test.perf;

import java.io.File;

import krati.cds.impl.store.PartitionedDataStore;
import krati.cds.store.DataStore;

public class EvalPartitionedStore extends EvalDataStore
{
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception
    {
        return new PartitionedDataStore(storeDir, 5, 1000000, 200000);
    }
    
    public static void main(String[] args)
    {
        new EvalPartitionedStore().run(4, 2);
        System.out.println("done");
    }
}
