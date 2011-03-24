package test.util;

import java.util.List;

import krati.store.ArrayStorePartition;

/**
 * DataPartitionChecker
 * 
 * @author jwu
 * 
 */
public class DataPartitionChecker extends DataPartitionReader {
    
    public DataPartitionChecker(ArrayStorePartition partition, List<String> seedData) {
        super(partition, seedData);
    }
    
    void check(int index) {
        String line = _lineSeedData.get(index % _lineSeedData.size());
        
        byte[] b = _partition.get(index);
        if (b != null) {
            String s = new String(b);
            if (!s.equals(line)) {
                throw new RuntimeException("[" + index + "]=" + s + " expected=" + line);
            }
        } else {
            if (line != null) {
                throw new RuntimeException("[" + index + "]=null expected=" + line);
            }
        }
    }
    
    @Override
    public void run() {
        while (_running) {
            int index = _indexStart + _rand.nextInt(_length);
            check(index);
            _cnt++;
        }
    }
}
