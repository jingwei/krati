package test.util;

import java.util.List;

import krati.store.DataSet;

/**
 * DataSetChecker
 * 
 * @author jwu
 * 
 */
public class DataSetChecker extends DataSetRunner {
    
    public DataSetChecker(DataSet<byte[]> store, List<String> seedData, int keyCount) {
        super(store, seedData, keyCount);
    }
    
    @Override
    protected void op() {
        int i = _rand.nextInt(_keyCount);
        String s = _lineSeedData.get(i%_lineSeedCount);
        String k = s.substring(0, 30) + i;
        if (!_store.has(k.getBytes())) {
            System.err.printf(getClass().getSimpleName() + ": value=\"%s\" not found%n", k);
        }
    }
}
