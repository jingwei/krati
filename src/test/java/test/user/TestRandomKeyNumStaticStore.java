package test.user;

import java.io.File;

import krati.store.DataStore;
import krati.store.StaticDataStore;

/**
 * TestRandomKeyNumDynamicStore
 * 
 * @author jwu
 * 06/09, 2011
 * 
 */
public class TestRandomKeyNumStaticStore extends TestRandomKeyNumStore {
    
    protected int getCapacity() {
        return Math.min((int)(getKeyCount() * 1.5), 200000000);
    }
    
    @Override
    protected DataStore<byte[], byte[]> createStore(File homeDir) throws Exception {
        return new StaticDataStore(homeDir, getCapacity(), getSegmentFileSizeMB(), createSegmentFactory());
    }
}
