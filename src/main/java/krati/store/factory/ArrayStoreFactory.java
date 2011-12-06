package krati.store.factory;

import java.io.IOException;

import krati.core.StoreConfig;
import krati.store.ArrayStore;

/**
 * ArrayStoreFactory defines the interface for creating a {@link ArrayStore} based on a store configuration.
 * 
 * @author jwu
 * @since 12/06, 2011
 */
public interface ArrayStoreFactory {

    /**
     * Creates a {@link ArrayStore} based on the specified store configuration.
     * 
     * @param config - the store configuration
     * @return the newly created array store. 
     * @throws IOException if the store cannot be created.
     */
    public ArrayStore create(StoreConfig config) throws IOException;
}
