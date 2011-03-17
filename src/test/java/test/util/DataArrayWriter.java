package test.util;

import java.util.List;
import java.util.Random;

import krati.array.DataArray;
import test.LatencyStats;

/**
 * DataArrayWriter
 * 
 * @author jwu
 * 
 */
public class DataArrayWriter implements Runnable {
    DataArray _dataArray;
    Random _rand = new Random();
    boolean _running = true;
    
    long _cnt = 0;
    LatencyStats _latStats = new LatencyStats();
    final List<String> _lineSeedData;
    
    public DataArrayWriter(DataArray dataArray, List<String> seedData) {
        this._dataArray = dataArray;
        this._lineSeedData = seedData;
    }
    
    public long getWriteCount() {
        return this._cnt;
    }

    public LatencyStats getLatencyStats() {
        return this._latStats;
    }
    
    public void stop() {
        _running = false;
    }
    
    void write(int index) {
        try {
            byte[] b = _lineSeedData.get(index % _lineSeedData.size()).getBytes();
            _dataArray.set(index, b, System.currentTimeMillis());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void run() {
        int length = _dataArray.length();
        long prevTime = System.nanoTime();
        long currTime = prevTime;
        
        while (_running) {
            write(_rand.nextInt(length));
            _cnt++;
            
            currTime = System.nanoTime();
            _latStats.countLatency((int)(currTime - prevTime)/1000);
            prevTime = currTime;
        }
    }
}
