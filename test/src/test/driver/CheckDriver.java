package test.driver;

import java.util.List;

public class CheckDriver<S> extends ReadDriver<S>
{
    public CheckDriver(S store, StoreReader<S, String, String> reader, List<String> lineSeedData)
    {
        super(store, reader, lineSeedData);
    }
    
    @Override
    protected void read()
    {
        String line = _lineSeedData.get(_rand.nextInt(_dataCnt));
        int keyLength = 30 + (_rand.nextInt(100) * 3);
        if(line.length() > keyLength) {
            String key = line.substring(0, keyLength);
            String val = _reader.get(_store, key);
            if(val != null) {
                String lineRead = val;
                if(!line.equals(lineRead)) {
                    System.err.printf("key=\"%s\"%n", key);
                    System.err.printf("    \"%s\"%n", line);
                    System.err.printf("    \"%s\"%n", lineRead);
                }
                
                if(!line.equals(lineRead)) {
                    throw new RuntimeException("key=" + key + ", value=" + line);
                }
            }
            _cnt++;
        }
    }
}
