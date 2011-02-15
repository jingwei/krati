package test.driver;

public interface StoreWriter<S, K, V>
{
    public void put(S store, K key, V value) throws Exception;
}
