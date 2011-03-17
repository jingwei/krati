package test.driver.string;

import java.util.List;

import test.driver.StoreReader;

/**
 * StringCheckDriver
 * 
 * @author jwu
 * 
 * @param <S> Data Store
 */
public class StringCheckDriver<S> extends StringReadDriver<S> {
    
    public StringCheckDriver(S store, StoreReader<S, String, String> reader, List<String> lineSeedData, int keyCount) {
        super(store, reader, lineSeedData, keyCount);
    }
    
    @Override
    protected void read() {
        int i = _rand.nextInt(_keyCount);
        String s = _lineSeedData.get(i%_lineSeedCount);
        String key = s.substring(0, 30) + i;
        String value = _reader.get(_store, key);
        
        if (value != null) {
            if (!s.equals(value)) {
                System.err.printf("key=\"%s\"%n", key);
                System.err.printf("    \"%s\"%n", s);
                System.err.printf("    \"%s\"%n", value);
            }
        } else {
            System.err.printf("check found null for key=\"%s\"%n", key);
        }
        _cnt++;
    }
}
