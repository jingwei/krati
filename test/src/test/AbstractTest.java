package test;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import junit.framework.TestCase;

public class AbstractTest extends TestCase
{
    public static final File TEST_DIR;
    public static final File TEST_OUTPUT_DIR;
    static final Logger _log = Logger.getLogger(AbstractTest.class);
    
    static
    {
        TEST_DIR = new File(System.getProperty("test.dir"));
        if(!TEST_DIR.exists())
        {
            TEST_DIR.mkdirs();
        }
        
        TEST_OUTPUT_DIR = new File(System.getProperty("test.output.dir"));
        if(!TEST_OUTPUT_DIR.exists())
        {
            TEST_OUTPUT_DIR.mkdirs();
        }
    }
    
    // Short-Regress Test Params.
    public static int idStart = 0;
    public static int idCount = 500000;
    public static int runTimeSeconds = 120;
    public static int segFileSizeMB = 256;
    
    static
    {
        try
        {
            idStart = Integer.parseInt(System.getProperty("test.idStart"));
        }
        catch(Exception e)
        {
            idStart = 0;
        }
        finally
        {
            _log.info("test.idStart: " + idStart);
        }
        
        try
        {
            idCount = Integer.parseInt(System.getProperty("test.idCount"));
        }
        catch(Exception e)
        {
            idCount = 500000;
        }
        finally
        {
            _log.info("test.idCount: " + idCount);
        }
        
        try
        {
            runTimeSeconds = Integer.parseInt(System.getProperty("test.runTimeSeconds"));
        }
        catch(Exception e)
        {
            runTimeSeconds = 120;
        }
        finally
        {
            _log.info("test.runTimeSeconds: " + runTimeSeconds);
        }
        
        try
        {
            segFileSizeMB = Integer.parseInt(System.getProperty("test.segFileSizeMB"));
        }
        catch(Exception e)
        {
            segFileSizeMB = 256;
        }
        finally
        {
            _log.info("test.segFileSizeMB: " + segFileSizeMB);
        }
    }
    
    protected String name;
    
    protected AbstractTest(String name)
    {
        this.name = name;
    }
    
    public void cleanTestOutput() throws Exception
    {
        File[] files = TEST_OUTPUT_DIR.listFiles();
        
        for (File f : files)
        {
            if (f.isFile())
            {
                boolean deleted = f.delete();
                if (!deleted)
                {
                    throw new IOException("file:"+f.getAbsolutePath()+" not deleted");
                }
            }
            else
            {
                deleteDirectory(f);
            }
        }
    }
    
    public void cleanDirectory(File dir) throws IOException
    {
        File[] files = dir.listFiles();
        
        for (File f : files)
        {
            if (f.isFile())
            {
                boolean deleted = f.delete();
                if (!deleted)
                {
                    throw new IOException("file:"+f.getAbsolutePath()+" not deleted");
                }
            }
            else
            {
                deleteDirectory(f);
            }
        }
    }
    
    public void deleteDirectory(File dir) throws IOException
    {
        File[] files = dir.listFiles();
        
        for (File f : files)
        {
            if (f.isDirectory())
            {
               deleteDirectory(f);
            }
            else
            {
                boolean deleted = f.delete();
                if (!deleted)
                {
                    throw new IOException("file:"+f.getAbsolutePath()+" not deleted");
                }
            }
        }
        
        boolean deleted = dir.delete();
        if (!deleted)
        {
            throw new IOException("dir:"+dir.getAbsolutePath()+" not deleted");
        }
    }
}
