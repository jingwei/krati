package krati.store.factory;

import java.io.IOException;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.io.Serializer;
import krati.store.ArrayStore;
import krati.store.ObjectStore;
import krati.store.SerializableObjectArray;

/**
 * DynamicObjectArrayFactory
 * 
 * @author jwu
 * @since 10/11, 2011
 */
public class DynamicObjectArrayFactory<V> implements ObjectStoreFactory<Integer, V> {
    
    @Override
    public ObjectStore<Integer, V> create(StoreConfig config, Serializer<Integer> keySerializer, Serializer<V> valueSerializer) throws IOException {
        try {
            ArrayStore base = StoreFactory.createDynamicArrayStore(config);
            return new SerializableObjectArray<V>(base, keySerializer, valueSerializer);
        } catch (Exception e) {
            if(e instanceof IOException) {
                throw (IOException)e;
            } else {
                throw new IOException(e);
            }
        }
    }
}
