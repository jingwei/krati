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
    
    // Short-Regression Test Params.
    public static int _idStart = 0;
    public static int _idCount = 500000;
    public static int _keyCount = 375000;
    public static int _runTimeSeconds = 120;
    public static int _segFileSizeMB = 256;
    public static int _initLevel = 5;
    public static int _numReaders = 4;
    
    static
    {
        try
        {
            _idStart = Integer.parseInt(System.getProperty("test.idStart"));
        }
        catch(Exception e)
        {
            _idStart = 0;
        }
        finally
        {
            _log.info("test.idStart: " + _idStart);
        }
        
        try
        {
            _idCount = Integer.parseInt(System.getProperty("test.idCount"));
        }
        catch(Exception e)
        {
            _idCount = 500000;
        }
        finally
        {
            _log.info("test.idCount: " + _idCount);
        }
        
        try
        {
            _keyCount = Integer.parseInt(System.getProperty("test.keyCount"));
        }
        catch(Exception e)
        {
            _keyCount = (int)(_idCount * 0.75);
        }
        finally
        {
            _log.info("test.keyCount: " + _keyCount);
        }
        
        try
        {
            _runTimeSeconds = Integer.parseInt(System.getProperty("test.runTimeSeconds"));
        }
        catch(Exception e)
        {
            _runTimeSeconds = 120;
        }
        finally
        {
            _log.info("test.runTimeSeconds: " + _runTimeSeconds);
        }
        
        try
        {
            _segFileSizeMB = Integer.parseInt(System.getProperty("test.segFileSizeMB"));
        }
        catch(Exception e)
        {
            _segFileSizeMB = 256;
        }
        finally
        {
            _log.info("test.segFileSizeMB: " + _segFileSizeMB);
        }
        
        try
        {
            _initLevel = Integer.parseInt(System.getProperty("test.initLevel"));
        }
        catch(Exception e)
        {
            _initLevel = 5;
        }
        finally
        {
            _log.info("test.initLevel: " + _initLevel);
        }
        
        try
        {
            _numReaders = Integer.parseInt(System.getProperty("test.numReaders"));
        }
        catch(Exception e)
        {
            _numReaders = 4;
        }
        finally
        {
            _log.info("test.numReaders: " + _numReaders);
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
    
    protected File getHomeDirectory()
    {
        return new File(TEST_OUTPUT_DIR, getClass().getSimpleName());
    }
}
