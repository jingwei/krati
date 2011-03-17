package test.driver;

/**
 * StoreReader
 * 
 * @author jwu
 * 
 * @param <S> Store
 * @param <K> Key
 * @param <V> Value
 */
public interface StoreReader<S, K, V> {
    public V get(S store, K key);
}
