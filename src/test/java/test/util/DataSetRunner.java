package test.util;

import java.util.List;
import java.util.Random;

import krati.store.DataSet;
import test.LatencyStats;

/**
 * DataSetRunner
 * 
 * @author jwu
 * 
 */
public abstract class DataSetRunner implements Runnable {
    DataSet<byte[]> _store;
    Random _rand = new Random();
    boolean _running = true;
    long _cnt = 0;
    
    LatencyStats _latStats = new LatencyStats();
    final List<String> _lineSeedData;
    final int _lineSeedCount;
    final int _keyCount;
    
    public DataSetRunner(DataSet<byte[]> store, List<String> seedData, int keyCount) {
        this._store = store;
        this._lineSeedData = seedData;
        this._lineSeedCount = seedData.size();
        this._keyCount = keyCount;
    }
    
    public long getOpCount() {
        return this._cnt;
    }
    
    public LatencyStats getLatencyStats() {
        return this._latStats;
    }
    
    public void stop() {
        _running = false;
    }
    
    protected abstract void op();
    
    @Override
    public void run() {
        long prevTime = System.nanoTime();
        long currTime = prevTime;
        
        while (_running) {
            op();
            _cnt++;
            
            currTime = System.nanoTime();
            _latStats.countLatency((int)(currTime - prevTime)/1000);
            prevTime = currTime;
        }
    }
}
