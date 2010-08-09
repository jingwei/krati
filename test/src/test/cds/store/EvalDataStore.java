package test.cds.store;

import java.io.File;

import krati.cds.store.DataStore;
import test.AbstractSeedTest;
import test.driver.StoreReader;
import test.driver.StoreTestDriver;
import test.driver.StoreWriter;
import test.driver.raw.StoreTestBytesDriver;

public abstract class EvalDataStore extends AbstractSeedTest
{
    public EvalDataStore(String name)
    {
        super(name);
    }
    
    protected abstract DataStore<byte[], byte[]> getDataStore(File DataStoreDir) throws Exception;
    
    static class DataStoreReader implements StoreReader<DataStore<byte[], byte[]>, byte[], byte[]>
    {
        @Override
        public final byte[] get(DataStore<byte[], byte[]> store, byte[] key)
        {
            return (key == null) ? null : store.get(key);
        }
    }
    
    static class DataStoreWriter implements StoreWriter<DataStore<byte[], byte[]>, byte[], byte[]>
    {
        @Override
        public final void put(DataStore<byte[], byte[]> store, byte[] key, byte[] value) throws Exception
        {
            store.put(key, value);
        }
    }
    
    public void evalPerformance(int numOfReaders, int numOfWriters, int runDuration) throws Exception
    {
        try
        {
            AbstractSeedTest.loadSeedData();
        }
        catch(Exception e)
        {
            e.printStackTrace();
            return;
        }
        
        File storeDir = getHomeDirectory();
        if(!storeDir.exists()) storeDir.mkdirs();
        cleanDirectory(storeDir);
        
        DataStore<byte[], byte[]> store = getDataStore(storeDir);
        StoreReader<DataStore<byte[], byte[]>, byte[], byte[]> storeReader = new DataStoreReader();
        StoreWriter<DataStore<byte[], byte[]>, byte[], byte[]> storeWriter = new DataStoreWriter();
        
        StoreTestDriver driver;
        driver = new StoreTestBytesDriver<DataStore<byte[], byte[]>>(store, storeReader, storeWriter, _lineSeedData, _keyCount);
        driver.run(numOfReaders, numOfWriters, runDuration);
        
        store.sync();
    }
}
