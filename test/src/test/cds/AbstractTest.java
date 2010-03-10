package test.cds;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import junit.framework.TestCase;

public class AbstractTest extends TestCase
{
    public static final File TEST_OUTPUT_DIR;
    public static final Logger log = Logger.getLogger(AbstractTest.class);
    
    static
    {
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
        }
    }
}
