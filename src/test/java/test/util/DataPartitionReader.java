package test.util;

import java.util.List;
import java.util.Random;

import krati.store.ArrayStorePartition;
import test.LatencyStats;

/**
 * DataPartitionReader
 * 
 * @author jwu
 * 
 */
public class DataPartitionReader implements Runnable {
    ArrayStorePartition _partition;
    Random _rand = new Random();
    byte[] _data = new byte[1 << 13];
    boolean _running = true;
    int _indexStart;
    int _length;
    long _cnt = 0;
    
    LatencyStats _latStats = new LatencyStats();
    final List<String> _lineSeedData;
    
    public DataPartitionReader(ArrayStorePartition partition, List<String> seedData) {
        this._partition = partition;
        this._length = partition.getIdCount();
        this._indexStart = partition.getIdStart();
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
        return _partition.get(index, _data);
    }
    
    @Override
    public void run() {
        long prevTime = System.nanoTime();
        long currTime = prevTime;
        
        while (_running) {
            read(_indexStart + _rand.nextInt(_length));
            _cnt++;
            
            currTime = System.nanoTime();
            _latStats.countLatency((int)(currTime - prevTime)/1000);
            prevTime = currTime;
        }
    }
}
