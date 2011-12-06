package krati.store.factory;

import java.io.IOException;

import krati.core.StoreConfig;
import krati.store.DataStore;

/**
 * DataStoreFactory defines the interface for creating a {@link DataStore} with
 * keys and values in the form of byte array.
 * 
 * @author jwu
 * @since 12/05, 2011
 */
public interface DataStoreFactory {
    
    /**
     * Creates a {@link DataStore} with keys and values in the form of byte array.
     * 
     * @param config - the store configuration
     * @return the newly created store. 
     * @throws IOException if the store cannot be created.
     */
    public DataStore<byte[], byte[]> create(StoreConfig config) throws IOException;
}
