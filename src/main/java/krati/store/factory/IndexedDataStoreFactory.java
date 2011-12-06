package krati.store.factory;

import java.io.IOException;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.store.IndexedDataStore;

/**
 * IndexedDataStoreFactory creates a {@link IndexedDataStore} with keys and values
 * in the form of byte array.
 * 
 * @author jwu
 * @since 12/05, 2011
 */
public class IndexedDataStoreFactory implements BasicStoreFactory {
    
    /**
     * Creates a {@link IndexedDataStore} with keys and values in the form of byte array.
     * 
     * @param config - the store configuration
     * @return the newly created IndexedDataStore. 
     * @throws IOException if the store cannot be created.
     */
    @Override
    public IndexedDataStore create(StoreConfig config) throws IOException {
        try {
            return StoreFactory.createIndexedDataStore(config);
        } catch (Exception e) {
            if(e instanceof IOException) {
                throw (IOException)e;
            } else {
                throw new IOException(e);
            }
        }
    }
}
