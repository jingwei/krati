package krati.store.factory;

import java.io.IOException;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.store.ArrayStore;

/**
 * DynamicArrayStoreFactory creates a dynamic array store using 
 * {@link krati.store.DynamicDataArray DynamicDataArray}.
 * 
 * @author jwu
 * @since 12/06, 2011
 */
public class DynamicArrayStoreFactory implements ArrayStoreFactory {
    
    /**
     * Creates a {@link ArrayStore} based on the specified store configuration.
     * 
     * @param config - the store configuration
     * @return the newly created array store in the form of {@link krati.store.DynamicDataArray DynamicDataArray}.
     * @throws IOException if the store cannot be created.
     */
    @Override
    public ArrayStore create(StoreConfig config) throws IOException {
        try {
            return StoreFactory.createDynamicArrayStore(config);
        } catch (Exception e) {
            if(e instanceof IOException) {
                throw (IOException)e;
            } else {
                throw new IOException(e);
            }
        }
    }
}
