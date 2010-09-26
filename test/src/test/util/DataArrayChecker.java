package test.util;

import java.util.List;

import krati.array.DataArray;

public class DataArrayChecker extends DataArrayReader
{
    public DataArrayChecker(DataArray dataArray, List<String> seedData)
    {
        super(dataArray, seedData);
    }
    
    void check(int index)
    {
        String line = _lineSeedData.get(index % _lineSeedData.size());
        
        byte[] b = _dataArray.getData(index);
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
        int length = _dataArray.length();
        while(_running)
        {
            int index = _rand.nextInt(length);
            check(index);
            _cnt++;
        }
    }
}
