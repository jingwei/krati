package test.bdb;

import test.driver.StoreReader;

import com.sleepycat.collections.StoredMap;

/**
 * BdbStringReader
 * 
 * @author jwu
 * 
 */
public class BdbStringReader implements StoreReader<StoredMap<String, String>, String, String> {
    @Override
    public final String get(StoredMap<String, String> store, String key) {
        return store.get(key);
    }
}
