package test.store.api;

import java.io.File;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.store.ArrayStore;

/**
 * TestStaticDataArrayIterator
 * 
 * @author jwu
 * @since 10/08, 2011
 */
public class TestStaticDataArrayIterator extends AbstractTestArrayStoreIterator {
    
    @Override
    protected ArrayStore createStore(File homeDir) throws Exception {
        StoreConfig config = new StoreConfig(homeDir, 1000);
        return StoreFactory.createStaticArrayStore(config);
    }
}
