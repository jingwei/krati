package test.driver;

import java.util.List;
import java.util.Random;

import test.LatencyStats;

/**
 * Write driver for data store.
 * 
 * @author jwu
 *
 * @param <S> Data Store
 */
public class WriteDriver<S> implements Runnable
{
    private final S _store;
    private final StoreWriter<S, String, String> _writer;
    private final LatencyStats _latencyStats = new LatencyStats();
    private final Random _rand = new Random();
    private final List<String> _lineSeedData;
    private final int _dataCnt;
    
    volatile long _cnt = 0;
    volatile boolean _running = true;
    
    public WriteDriver(S ds, StoreWriter<S, String, String> writer, List<String> lineSeedData)
    {
        this._store = ds;
        this._writer = writer;
        this._lineSeedData = lineSeedData;
        this._dataCnt = _lineSeedData.size();
    }
    
    public LatencyStats getLatencyStats()
    {
        return this._latencyStats;
    }
    
    public long getWriteCount()
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
            write();

            currTime = System.nanoTime();
            _latencyStats.countLatency((int)(currTime - prevTime)/1000);
            prevTime = currTime;
        }
    }

    protected void write()
    {
        try
        {
            String value = _lineSeedData.get(_rand.nextInt(_dataCnt));
            int keyLength = 30 + ( _rand.nextInt(100) * 3);
            if(value.length() > keyLength) {
                String key = value.substring(0, keyLength);
                _writer.put(_store, key, value);
                _cnt++;
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}