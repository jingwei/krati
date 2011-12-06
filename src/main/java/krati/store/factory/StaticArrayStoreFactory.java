package krati.store.factory;

import java.io.IOException;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.StorePartitionConfig;
import krati.store.ArrayStore;

/**
 * StaticArrayStoreFactory creates a static array store using 
 * {@link krati.store.StaticDataArray StaticDataArray} or
 * {@link krati.store.StaticArrayStorePartition StaticArrayStorePartition}
 * 
 * @author jwu
 * @since 12/06, 2011
 */
public class StaticArrayStoreFactory implements ArrayStoreFactory {
    
    /**
     * Creates a {@link ArrayStore} based on the specified store configuration.
     * 
     * @param config - the store configuration
     * @return the newly created array store in the form of
     *         {@link krati.store.StaticDataArray StaticDataArray} or
     *         {@link krati.store.StaticArrayStorePartition StaticArrayStorePartition}
     *         depending on {@link StoreConfig} or {@link StorePartitionConfig}.
     * @throws IOException if the store cannot be created.
     */
    @Override
    public ArrayStore create(StoreConfig config) throws IOException {
        try {
            return (config instanceof StorePartitionConfig) ?
                    StoreFactory.createArrayStorePartition((StorePartitionConfig)config) :
                    StoreFactory.createStaticArrayStore(config);
        } catch (Exception e) {
            if(e instanceof IOException) {
                throw (IOException)e;
            } else {
                throw new IOException(e);
            }
        }
    }
}
