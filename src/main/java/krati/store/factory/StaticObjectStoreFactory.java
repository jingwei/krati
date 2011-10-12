package krati.store.factory;

import java.io.IOException;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.io.Serializer;
import krati.store.DataStore;
import krati.store.ObjectStore;
import krati.store.SerializableObjectStore;

/**
 * StaticObjectStoreFactory
 * 
 * @author jwu
 * @since 10/11, 2011
 */
public class StaticObjectStoreFactory<K, V> implements ObjectStoreFactory<K, V> {
    
    @Override
    public ObjectStore<K, V> create(StoreConfig config,
                                    Serializer<K> keySerializer,
                                    Serializer<V> valueSerializer) throws IOException {
        try {
            DataStore<byte[], byte[]> base = StoreFactory.createStaticDataStore(config);
            return new SerializableObjectStore<K, V>(base, keySerializer, valueSerializer);
        } catch (Exception e) {
            if(e instanceof IOException) {
                throw (IOException)e;
            } else {
                throw new IOException(e);
            }
        }
    }
}
