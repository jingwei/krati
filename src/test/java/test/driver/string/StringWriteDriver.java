package test.driver.string;

import java.util.List;
import java.util.Random;

import test.LatencyStats;
import test.driver.StoreWriter;

/**
 * Write driver for data store.
 * 
 * @author jwu
 *
 * @param <S> Data Store
 */
public class StringWriteDriver<S> implements Runnable {
    private final S _store;
    private final StoreWriter<S, String, String> _writer;
    private final LatencyStats _latencyStats = new LatencyStats();
    private final Random _rand = new Random();
    private final List<String> _lineSeedData;
    private final int _lineSeedCount;
    private final int _keyCount;
    
    volatile long _cnt = 0;
    volatile boolean _running = true;
    
    public StringWriteDriver(S ds, StoreWriter<S, String, String> writer, List<String> lineSeedData, int keyCount) {
        this._store = ds;
        this._writer = writer;
        this._lineSeedData = lineSeedData;
        this._lineSeedCount = lineSeedData.size();
        this._keyCount = keyCount;
    }
    
    public LatencyStats getLatencyStats() {
        return this._latencyStats;
    }
    
    public long getWriteCount() {
        return this._cnt;
    }
    
    public void stop() {
        _running = false;
    }
    
    @Override
    public void run() {
        long prevTime = System.nanoTime();
        long currTime = prevTime;
        
        while (_running) {
            write();

            currTime = System.nanoTime();
            _latencyStats.countLatency((int) (currTime - prevTime) / 1000);
            prevTime = currTime;
        }
    }

    protected void write() {
        try {
            int i = _rand.nextInt(_keyCount);
            String s = _lineSeedData.get(i % _lineSeedCount);
            String key = s.substring(0, 30) + i;
            _writer.put(_store, key, s);
            _cnt++;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
