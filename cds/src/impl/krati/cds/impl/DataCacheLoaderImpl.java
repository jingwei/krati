package krati.cds.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import krati.cds.DataCache;
import krati.cds.DataCacheLoader;

import org.apache.log4j.Logger;

/**
 * DataCache loader.
 * 
 * @author jwu
 *
 */
public class DataCacheLoaderImpl implements DataCacheLoader
{
    private final static Logger _log = Logger.getLogger(DataCacheLoaderImpl.class);
    
    @Override
    public void load(DataCache cache, File dataFile) throws IOException
    {
        String line;
        FileReader reader = new FileReader(dataFile);
        BufferedReader in = new BufferedReader(reader);

        int index = cache.getIdStart();
        int stopIndex = index + cache.getIdCount();
        
        while((line = in.readLine()) != null && index < stopIndex)
        {
            try
            {
                cache.setData(index, line.getBytes(), index);
            }
            catch(Exception e)
            {
                _log.error("index=" + index + ": " + e.getMessage());
                e.printStackTrace();
            }
            index++;
        }
        
        in.close();
        reader.close();
        cache.persist();
    }
    
    @Override
    public void dump(DataCache cache, File dumpFile) throws IOException
    {
        byte[] data;
        String line;
        FileOutputStream fos = new FileOutputStream(dumpFile);
        PrintWriter out = new PrintWriter(fos);
        
        for(int index = cache.getIdStart(), cnt = cache.getIdCount(); index < cnt; index++)
        {
            data = cache.getData(index);
            if(data != null)
            {
                line = new String(data);
                out.println(line);
            }
            else
            {
                out.println();
            }
            
            if(index % 10000 == 0) out.flush();
        }
        
        out.flush();
        out.close();
        fos.close();
    }
}
