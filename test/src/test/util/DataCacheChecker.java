package test.util;

import java.util.List;

import krati.cds.DataCache;

public class DataCacheChecker extends DataCacheReader
{
    public DataCacheChecker(DataCache cache, List<String> seedData)
    {
        super(cache, seedData);
    }
    
    void check(int index)
    {
        String line = _lineSeedData.get(index % _lineSeedData.size());
        
        byte[] b = _cache.getData(index);
        if (b != null)
        {
            String s = new String(b);
            if(!s.equals(line))
            {
                throw new RuntimeException("[" + index + "]=" + s + " expected=" + line);
            }
        }
        else
        {
            if(line != null)
            {
                throw new RuntimeException("[" + index + "]=null expected=" + line);
            }
        }
    }
    
    @Override
    public void run()
    {
        while(_running)
        {
            int index = _indexStart + _rand.nextInt(_length);
            check(index);
            _cnt++;
        }
    }
}
