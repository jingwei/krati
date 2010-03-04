package krati.mds.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import krati.mds.MDSCache;
import krati.mds.MDSLoader;

import org.apache.log4j.Logger;

/**
 * MDS data loader.
 *  
 * @author jwu
 *
 */
public class MDSLoaderImpl implements MDSLoader
{
    private final static Logger _log = Logger.getLogger(MDSLoaderImpl.class);
    
    @Override
    public void load(MDSCache mds, File dataFile) throws IOException
    {
        String line;
        FileReader reader = new FileReader(dataFile);
        BufferedReader in = new BufferedReader(reader);

        int index = mds.getIdStart();
        int stopIndex = index + mds.getIdCount();
        
        while((line = in.readLine()) != null && index < stopIndex)
        {
            try
            {
                mds.setData(index, line.getBytes(), index);
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
        mds.persist();
    }
    
    @Override
    public void dump(MDSCache mds, File dumpFile) throws IOException
    {
        byte[] data;
        String line;
        FileOutputStream fos = new FileOutputStream(dumpFile);
        PrintWriter out = new PrintWriter(fos);
        
        for(int index = mds.getIdStart(), cnt = mds.getIdCount(); index < cnt; index++)
        {
            data = mds.getData(index);
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
