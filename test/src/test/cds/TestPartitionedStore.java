package test.cds;

import java.io.File;

import krati.cds.impl.store.PartitionedDataStore;
import krati.cds.store.DataStore;

/**
 * Test Partitioned DataStore.
 * 
 * @author jwu
 *
 */
public class TestPartitionedStore extends EvalDataStore
{
    public TestPartitionedStore()
    {
        super(TestPartitionedStore.class.getName());
    }
    
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception
    {
        int partitionCapacity = idCount/5;
        return new PartitionedDataStore(storeDir, 5, partitionCapacity);
    }
    
    public void testPartitionedStore() throws Exception
    {
        new TestPartitionedStore().run(4, 2);
        System.out.println("done");
        cleanTestOutput();
    }
}
