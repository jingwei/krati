package krati.store.factory;

import java.io.IOException;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.store.StaticDataStore;

/**
 * StaticDataStoreFactory creates a {@link StaticDataStore} with keys and values
 * in the form of byte array.
 * 
 * @author jwu
 * @since 12/05, 2011
 */
public class StaticDataStoreFactory implements BasicStoreFactory {
    
    /**
     * Creates a {@link StaticDataStore} with keys and values in the form of byte array.
     * 
     * @param config - the store configuration
     * @return the newly created StaticDataStore. 
     * @throws IOException if the store cannot be created.
     */
    @Override
    public StaticDataStore create(StoreConfig config) throws IOException {
        try {
            return StoreFactory.createStaticDataStore(config);
        } catch (Exception e) {
            if(e instanceof IOException) {
                throw (IOException)e;
            } else {
                throw new IOException(e);
            }
        }
    }
}
