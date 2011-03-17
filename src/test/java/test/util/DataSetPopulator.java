package test.util;

import java.util.List;

import krati.store.DataSet;

/**
 * DataSetPopulator
 * 
 * @author jwu
 * 
 */
public class DataSetPopulator extends DataSetRunner {
    
    public DataSetPopulator(DataSet<byte[]> store, List<String> seedData, int keyCount) {
        super(store, seedData, keyCount);
    }
    
    @Override
    protected void op() {
        try {
            int i = _rand.nextInt(_keyCount);
            String s = _lineSeedData.get(i % _lineSeedCount);
            String k = s.substring(0, 30) + i;
            _store.add(k.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
