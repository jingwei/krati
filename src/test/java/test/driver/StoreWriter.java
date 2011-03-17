package test.driver;

/**
 * StoreWriter
 * 
 * @author jwu
 * 
 * @param <S> Store
 * @param <K> Key
 * @param <V> Value
 */
public interface StoreWriter<S, K, V> {
    public void put(S store, K key, V value) throws Exception;
}
