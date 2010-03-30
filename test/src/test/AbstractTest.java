package test;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import junit.framework.TestCase;

public class AbstractTest extends TestCase
{
    public static final File TEST_DIR;
    public static final File TEST_OUTPUT_DIR;
    public static final Logger log = Logger.getLogger(AbstractTest.class);
    
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
    
    protected String name;
    
    public AbstractTest(String name)
    {
        this.name = name;
    }
    
    protected void cleanTestOutput() throws Exception
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
    
    protected void deleteDirectory(File dir) throws IOException
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
