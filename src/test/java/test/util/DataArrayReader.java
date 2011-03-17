package test.util;

import java.util.List;
import java.util.Random;

import krati.array.DataArray;
import test.LatencyStats;

/**
 * DataArrayReader
 * 
 * @author jwu
 * 
 */
public class DataArrayReader implements Runnable {
    DataArray _dataArray;
    Random _rand = new Random();
    byte[] _data = new byte[1 << 13];
    boolean _running = true;
    long _cnt = 0;
    
    LatencyStats _latStats = new LatencyStats();
    final List<String> _lineSeedData;
    
    public DataArrayReader(DataArray dataArray, List<String> seedData) {
        this._dataArray = dataArray;
        this._lineSeedData = seedData;
    }
    
    public long getReadCount() {
        return this._cnt;
    }
    
    public LatencyStats getLatencyStats() {
        return this._latStats;
    }
    
    public void stop() {
        _running = false;
    }
    
    int read(int index) {
        return _dataArray.get(index, _data);
    }
    
    @Override
    public void run() {
        int length = _dataArray.length();
        long prevTime = System.nanoTime();
        long currTime = prevTime;
        
        while (_running) {
            read(_rand.nextInt(length));
            _cnt++;
            
            currTime = System.nanoTime();
            _latStats.countLatency((int)(currTime - prevTime)/1000);
            prevTime = currTime;
        }
    }
}
