package test.mds;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import junit.framework.TestCase;

public class AbstractTest extends TestCase
{
  public static final Logger log    = Logger.getLogger(AbstractTest.class);
  public static final File   TEST_DATA_DIR;
  
  static
  {
    TEST_DATA_DIR = new File(System.getProperty("test.data.dir"));
  }
  
  protected String name;
  
  public AbstractTest(String name)
  {
    this.name = name;
  }
  
  protected void cleanCacheDir() throws Exception {
    File[] files = TEST_DATA_DIR.listFiles();
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
