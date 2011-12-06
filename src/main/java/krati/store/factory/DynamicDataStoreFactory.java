package krati.store.factory;

import java.io.IOException;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.store.DynamicDataStore;

/**
 * DynamicDataStoreFactory creates a {@link DynamicDataStore} with keys and values
 * in the form of byte array.
 * 
 * @author jwu
 * @since 12/05, 2011
 */
public class DynamicDataStoreFactory implements DataStoreFactory {
    
    /**
     * Creates a {@link DynamicDataStore} with keys and values in the form of byte array.
     * 
     * @param config - the store configuration
     * @return the newly created DynamicDataStore. 
     * @throws IOException if the store cannot be created.
     */
    @Override
    public DynamicDataStore create(StoreConfig config) throws IOException {
        try {
            return StoreFactory.createDynamicDataStore(config);
        } catch (Exception e) {
            if(e instanceof IOException) {
                throw (IOException)e;
            } else {
                throw new IOException(e);
            }
        }
    }
}
