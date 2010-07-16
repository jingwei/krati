package test.bdb;

import test.driver.StoreWriter;

import com.sleepycat.collections.StoredMap;

public class BdbStringWriter implements StoreWriter<StoredMap<String, String>, String, String>
{
    @Override
    public final void put(StoredMap<String, String> store, String key, String value)
    {
        store.put(key, value);
    }
}
