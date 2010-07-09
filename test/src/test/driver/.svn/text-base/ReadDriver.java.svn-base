package test.driver;

import java.util.List;
import java.util.Random;

import test.LatencyStats;

/**
 * Read driver for data store.
 * 
 * @author jwu
 *
 * @param <S> Data Store
 */
public class ReadDriver<S> implements Runnable
{
    protected final S _store;
    protected final StoreReader<S, String, String> _reader;
    protected final LatencyStats _latStats = new LatencyStats();
    protected final Random _rand = new Random();
    protected final List<String> _lineSeedData;
    protected final int _dataCnt;

    volatile long _cnt = 0;
    volatile boolean _running = true;
    
    public ReadDriver(S store, StoreReader<S, String, String> reader, List<String> lineSeedData)
    {
        this._store = store;
        this._reader = reader;
        this._lineSeedData = lineSeedData;
        this._dataCnt = _lineSeedData.size();
    }
    
    public LatencyStats getLatencyStats()
    {
        return this._latStats;
    }
    
    public long getReadCount()
    {
        return this._cnt;
    }
    
    public void stop()
    {
        _running = false;
    }
    
    @Override
    public void run()
    {
        long prevTime = System.nanoTime();
        long currTime = prevTime;
        
        while(_running)
        {
            read();

            currTime = System.nanoTime();
            _latStats.countLatency((int)(currTime - prevTime)/1000);
            prevTime = currTime;
        }
    }
    
    protected void read()
    {
        String key = _lineSeedData.get(_rand.nextInt(_dataCnt));
        int keyLength = 30 + (_rand.nextInt(100) * 3);
        if(key.length() > keyLength) {
            key = key.substring(0, keyLength);
            _reader.get(_store, key);
            _cnt++;
        }
    }
}
