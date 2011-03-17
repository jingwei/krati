package test.driver.raw;

import java.util.List;
import java.util.Random;

import test.LatencyStats;
import test.driver.StoreReader;

/**
 * Read driver for data store.
 * 
 * @author jwu
 *
 * @param <S> Data Store
 */
public class BytesReadDriver<S> implements Runnable {
    protected final S _store;
    protected final StoreReader<S, byte[], byte[]> _reader;
    protected final LatencyStats _latStats = new LatencyStats();
    protected final Random _rand = new Random();
    protected final List<String> _lineSeedData;
    protected final int _lineSeedCount;
    protected final int _keyCount;

    volatile long _cnt = 0;
    volatile boolean _running = true;

    public BytesReadDriver(S store, StoreReader<S, byte[], byte[]> reader, List<String> lineSeedData, int keyCount) {
        this._store = store;
        this._reader = reader;
        this._lineSeedData = lineSeedData;
        this._lineSeedCount = lineSeedData.size();
        this._keyCount = keyCount;
    }
    
    public LatencyStats getLatencyStats() {
        return this._latStats;
    }
    
    public long getReadCount() {
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
            read();

            currTime = System.nanoTime();
            _latStats.countLatency((int)(currTime - prevTime)/1000);
            prevTime = currTime;
        }
    }
    
    protected void read() {
        int i = _rand.nextInt(_keyCount);
        String s = _lineSeedData.get(i%_lineSeedCount);
        String k = s.substring(0, 30) + i;
        _reader.get(_store, k.getBytes());
        _cnt++;
    }
}
