package test.driver;

public interface StoreReader<S, K, V>
{
    public V get(S store, K key);
}
