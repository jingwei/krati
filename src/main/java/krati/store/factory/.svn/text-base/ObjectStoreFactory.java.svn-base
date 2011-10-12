package krati.store.factory;

import java.io.IOException;

import krati.core.StoreConfig;
import krati.io.Serializer;
import krati.store.ObjectStore;

/**
 * ObjectStoreFactory
 * 
 * @author jwu
 * @since 10/11, 2011
 */
public interface ObjectStoreFactory<K, V> {
    
    /**
     * Create an instance of ObjectStore for mapping keys to values.
     * 
     * @param config          - the configuration
     * @param keySerializer   - the serializer for keys
     * @param valueSerializer - the serializer for values
     * @return the newly created store
     * @throws IOException if the store cannot be created for any reasons.
     */
    public ObjectStore<K, V> create(StoreConfig config, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException;
}
