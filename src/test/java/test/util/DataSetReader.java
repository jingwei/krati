package test.util;

import java.util.List;

import krati.store.DataSet;

/**
 * DataSetReader
 * 
 * @author jwu
 * 
 */
public class DataSetReader extends DataSetRunner {
    public DataSetReader(DataSet<byte[]> store, List<String> seedData, int keyCount) {
        super(store, seedData, keyCount);
    }
    
    @Override
    protected void op() {
        int i = _rand.nextInt(_keyCount);
        String s = _lineSeedData.get(i%_lineSeedCount);
        String k = s.substring(0, 30) + i;
        _store.has(k.getBytes());
    }
}
