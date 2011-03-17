package test.driver.raw;

import java.util.Arrays;
import java.util.List;

import test.driver.StoreReader;

/**
 * BytesCheckDriver
 * 
 * @author jwu
 * 
 * @param <S> Data Store
 */
public class BytesCheckDriver<S> extends BytesReadDriver<S> {
    
    public BytesCheckDriver(S store, StoreReader<S, byte[], byte[]> reader, List<String> lineSeedData, int keyCount) {
        super(store, reader, lineSeedData, keyCount);
    }
    
    @Override
    protected void read() {
        int i = _rand.nextInt(_keyCount);
        String s = _lineSeedData.get(i%_lineSeedCount);
        String k = s.substring(0, 30) + i;
        
        byte[] key = k.getBytes();
        byte[] value = _reader.get(_store, key);
        if (value != null) {
            if (!Arrays.equals(s.getBytes(), value)) {
                System.err.printf("key=\"%s\"%n", k);
                System.err.printf("    \"%s\"%n", s);
                System.err.printf("    \"%s\"%n", new String(value));
            }
        } else {
            System.err.printf("check found null for key=\"%s\"%n", key);
        }
        _cnt++;
    }
}
