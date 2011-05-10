package test.store;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;

import krati.core.segment.SegmentFactory;
import krati.store.DataStore;
import test.AbstractTest;
import test.StatsLog;

/**
 * EvalDataStoreIterator
 * 
 * @author jwu
 * Sep 30, 2010
 */
public abstract class EvalDataStoreIterator extends AbstractTest {
    private final DataStore<byte[], byte[]> _store;
    
    protected EvalDataStoreIterator(String name) throws Exception {
        super(name);
        _store = createDataStore(getHomeDirectory());
    }
    
    protected SegmentFactory createSegmentFactory() {
        return new krati.core.segment.MemorySegmentFactory();
    }
    
    protected abstract DataStore<byte[], byte[]> createDataStore(File storeDir) throws Exception;
    
    protected void populate() throws Exception {
        StatsLog.logger.info(">>> populate");
        
        int count = 0;
        long startTime = System.currentTimeMillis();
        int lineSeedCount = _lineSeedData.size();
        
        for (int i = 0; i < _keyCount; i++) {
            String s = _lineSeedData.get(i%lineSeedCount);
            String k = s.substring(0, 30) + i;
            _store.put(k.getBytes(), s.getBytes());
            count++;
        }
        
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        StatsLog.logger.info("elapsedTime=" + elapsedTime + " ms");
        
        double rate;
        rate = count/(double)elapsedTime;
        rate = Math.round(rate * 100) / 100.0;
        StatsLog.logger.info("writeCount=" + count + " rate=" + rate + " per ms");
    }
    
    public void testIterator() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);

        populate();
        
        checkKeyIterator();
        
        checkEntryIterator();
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
    
    protected void checkKeyIterator() {
        int cnt = 0;
        long start = System.currentTimeMillis();
        Iterator<byte[]> iter = _store.keyIterator();
        StatsLog.logger.info(">>> iterate keys");
        
        byte[] key = null;
        while (iter.hasNext()) {
            key = iter.next();
            _store.get(key);
            cnt++;
        }
        
        StatsLog.logger.info("read " + cnt + " key-value(s) in " + (System.currentTimeMillis() - start) + " ms");
        assertEquals(_keyCount, cnt);
        StatsLog.logger.info("OK");
    }
    
    protected void checkEntryIterator() {
        int cnt = 0;
        long start = System.currentTimeMillis();
        StatsLog.logger.info(">>> iterate entries");
        
        for(Entry<byte[], byte[]> e : _store) {
            byte[] value = _store.get(e.getKey());
            assertTrue(Arrays.equals(e.getValue(), value));
            cnt++;
        }
        
        StatsLog.logger.info("read " + cnt + " key-value(s) in " + (System.currentTimeMillis() - start) + " ms");
        assertEquals(_keyCount, cnt);
        StatsLog.logger.info("OK");
    }
}
