package test.user;

import java.io.File;

import krati.core.StoreFactory;
import krati.store.DataStore;

/**
 * TestRandomKeyNumDynamicStore
 * 
 * @author jwu
 * 06/09, 2011
 * 
 */
public class TestRandomKeyNumStaticStore extends TestRandomKeyNumStore {
    
    @Override
    protected DataStore<byte[], byte[]> createStore(File homeDir) throws Exception {
        return StoreFactory.createStaticDataStore(
                homeDir,
                getCapacity(),
                getSegmentFileSizeMB(),
                createSegmentFactory());
    }
}
