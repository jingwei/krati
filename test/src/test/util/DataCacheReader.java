package test.util;

import java.util.List;
import java.util.Random;

import krati.store.DataCache;
import test.LatencyStats;

public class DataCacheReader implements Runnable
{
    DataCache _cache;
    Random _rand = new Random();
    byte[] _data = new byte[1 << 13];
    boolean _running = true;
    int _indexStart;
    int _length;
    long _cnt = 0;
    
    LatencyStats _latStats = new LatencyStats();
    final List<String> _lineSeedData;
    
    public DataCacheReader(DataCache cache, List<String> seedData)
    {
        this._cache = cache;
        this._length = cache.getIdCount();
        this._indexStart = cache.getIdStart();
        this._lineSeedData = seedData;
    }
    
    public long getReadCount()
    {
        return this._cnt;
    }
    
    public LatencyStats getLatencyStats()
    {
        return this._latStats;
    }
    
    public void stop()
    {
        _running = false;
    }
    
    int read(int index)
    {
        return _cache.get(index, _data);
    }
    
    @Override
    public void run()
    {
        long prevTime = System.nanoTime();
        long currTime = prevTime;
        
        while(_running)
        {
            read(_indexStart + _rand.nextInt(_length));
            _cnt++;
            
            currTime = System.nanoTime();
            _latStats.countLatency((int)(currTime - prevTime)/1000);
            prevTime = currTime;
        }
    }
}
