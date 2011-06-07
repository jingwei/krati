package test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import test.util.SeedData;

import junit.framework.TestCase;

/**
 * AbstractTest
 * 
 * @author jwu
 * 
 */
public class AbstractTest extends TestCase {
    public static final File TEST_OUTPUT_DIR;
    public static final File TEST_RESOURCES_DIR;
    static final Logger _log = Logger.getLogger(AbstractTest.class);
    
    static {
        TEST_OUTPUT_DIR = new File(System.getProperty("krati.test.output.dir"));
        if(!TEST_OUTPUT_DIR.exists()) {
            TEST_OUTPUT_DIR.mkdirs();
        }
        
        TEST_RESOURCES_DIR = new File(System.getProperty("krati.test.resources.dir"));
        if(!TEST_RESOURCES_DIR.exists()) {
            TEST_RESOURCES_DIR.mkdirs();
        }
        
        _log.info("krati.test.output.dir: " + TEST_OUTPUT_DIR.getAbsolutePath());
        _log.info("krati.test.resources.dir: " + TEST_RESOURCES_DIR.getAbsolutePath());
    }
    
    // Default Test Params.
    public static int _idStart = 0;
    public static int _idCount = 100000;
    public static int _keyCount = 75000;
    public static int _runTimeSeconds = 60;
    public static int _segFileSizeMB = 128;
    public static int _initLevel = 5;
    public static int _numReaders = 4;
    public static int _hitPercent = 100;
    
    static {
        try {
            _idStart = Integer.parseInt(System.getProperty("krati.test.idStart"));
        } catch(Exception e) {
            _idStart = 0;
        } finally {
            _log.info("krati.test.idStart: " + _idStart);
        }
        
        try {
            _idCount = Integer.parseInt(System.getProperty("krati.test.idCount"));
        } catch(Exception e) {
            _idCount = 100000;
        } finally {
            _log.info("krati.test.idCount: " + _idCount);
        }
        
        try {
            _keyCount = Integer.parseInt(System.getProperty("krati.test.keyCount"));
        } catch(Exception e) {
            _keyCount = (int)(_idCount * 0.75);
        } finally {
            _log.info("krati.test.keyCount: " + _keyCount);
        }
        
        try {
            _runTimeSeconds = Integer.parseInt(System.getProperty("krati.test.runTimeSeconds"));
        } catch(Exception e) {
            _runTimeSeconds = 60;
        } finally {
            _log.info("krati.test.runTimeSeconds: " + _runTimeSeconds);
        }
        
        try {
            _segFileSizeMB = Integer.parseInt(System.getProperty("krati.test.segFileSizeMB"));
        } catch(Exception e) {
            _segFileSizeMB = 128;
        } finally {
            _log.info("krati.test.segFileSizeMB: " + _segFileSizeMB);
        }
        
        try {
            _initLevel = Integer.parseInt(System.getProperty("krati.test.initLevel"));
        } catch(Exception e) {
            _initLevel = 5;
        } finally {
            _log.info("krati.test.initLevel: " + _initLevel);
        }
        
        try {
            _numReaders = Integer.parseInt(System.getProperty("krati.test.numReaders"));
        } catch(Exception e) {
            _numReaders = 4;
        } finally {
            _log.info("krati.test.numReaders: " + _numReaders);
        }
        
        try {
            _hitPercent = Integer.parseInt(System.getProperty("krati.test.hitPercent"));
            _hitPercent = Math.min(_hitPercent, 100);
        } catch(Exception e) {
            _hitPercent = 100;
        } finally {
            _log.info("krati.test.hitPercent: " + _hitPercent);
        }
    }
    
    protected String name;
    
    protected AbstractTest(String name) {
        this.name = name;
    }
    
    public File getHomeDirectory() {
        return new File(TEST_OUTPUT_DIR, getClass().getSimpleName());
    }
    
    public void cleanHomeDirectory() throws IOException {
        cleanDirectory(getHomeDirectory());
    }
    
    public void deleteHomeDirectory() throws IOException {
        deleteDirectory(getHomeDirectory());
    }
    
    public void cleanTestOutput() throws Exception {
        File[] files = TEST_OUTPUT_DIR.listFiles();
        
        for (File f : files) {
            if (f.isFile()) {
                boolean deleted = f.delete();
                if (!deleted) {
                    throw new IOException("file:"+f.getAbsolutePath()+" not deleted");
                }
            } else {
                deleteDirectory(f);
            }
        }
    }
    
    public static void cleanDirectory(File dir) throws IOException {
        File[] files = dir.listFiles();
        
        for (File f : files) {
            if (f.isFile()) {
                boolean deleted = f.delete();
                if (!deleted) {
                    throw new IOException("file:"+f.getAbsolutePath()+" not deleted");
                }
            } else {
                deleteDirectory(f);
            }
        }
    }
    
    public static void deleteDirectory(File dir) throws IOException {
        File[] files = dir.listFiles();
        
        for (File f : files) {
            if (f.isDirectory()) {
               deleteDirectory(f);
            } else {
                boolean deleted = f.delete();
                if (!deleted) {
                    throw new IOException("file:"+f.getAbsolutePath()+" not deleted");
                }
            }
        }
        
        boolean deleted = dir.delete();
        if (!deleted) {
            throw new IOException("dir:"+dir.getAbsolutePath()+" not deleted");
        }
    }
    
    public static int getIdStart() {
        return _idStart;
    }
    
    public static int getIdCount() {
        return _idCount;
    }
    
    public static int getKeyCount() {
        return _keyCount;
    }
    
    public static int getRunTimeSeconds() {
        return _runTimeSeconds;
    }
    
    public static int getSegFileSizeMB() {
        return _segFileSizeMB;
    }
    
    public static int getInitLevel() {
        return _initLevel;
    }
    
    public static int getNumReaders() {
        return _numReaders;
    }
    
    public static int gethitPercent() {
        return _hitPercent;
    }
    
    public static List<String> _lineSeedData = null;
    
    static {
        SeedData seedData = new SeedData();
        try {
            seedData.load();
            _lineSeedData = seedData.getLines();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
