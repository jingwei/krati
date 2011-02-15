package test.util;

import java.util.List;
import java.util.Random;

import krati.store.ArrayStorePartition;
import test.LatencyStats;

public class DataCacheWriter implements Runnable
{
    ArrayStorePartition _cache;
    Random _rand = new Random();
    boolean _running = true;
    
    int _indexStart;
    int _length;
    long _cnt = 0;
    long _scn = 0;
    LatencyStats _latStats = new LatencyStats();
    final List<String> _lineSeedData;
    
    public DataCacheWriter(ArrayStorePartition cache, List<String> seedData)
    {
        this._cache = cache;
        this._length = cache.getIdCount();
        this._indexStart = cache.getIdStart();
        this._scn = cache.getHWMark();
        this._lineSeedData = seedData;
    }
    
    public long getWriteCount()
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
    
    void write(int index)
    {
        try
        {
            byte[] b = _lineSeedData.get(index%_lineSeedData.size()).getBytes();
            _cache.set(index, b, _scn++);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
    
    @Override
    public void run()
    {
        long prevTime = System.nanoTime();
        long currTime = prevTime;
        
        while(_running)
        {
            write(_indexStart + _rand.nextInt(_length));
            _cnt++;
            
            currTime = System.nanoTime();
            _latStats.countLatency((int)(currTime - prevTime)/1000);
            prevTime = currTime;
        }
    }
}
