package test.util;

import java.io.File;
import java.io.IOException;

/**
 * FileUtils
 * 
 * @author jwu
 * 01/15, 2011
 */
public class FileUtils {
    public static final File TEST_OUTPUT_DIR;
    public static final File TEST_RESOURCES_DIR;
    
    static {
        TEST_OUTPUT_DIR = new File(System.getProperty("krati.test.output.dir"));
        if (!TEST_OUTPUT_DIR.exists()) {
            TEST_OUTPUT_DIR.mkdirs();
        }
        
        TEST_RESOURCES_DIR = new File(System.getProperty("krati.test.resources.dir"));
        if (!TEST_RESOURCES_DIR.exists()) {
            TEST_RESOURCES_DIR.mkdirs();
        }
    }
    
    public static File getTestFile(String fileName) throws IOException {
        File file = new File(TEST_OUTPUT_DIR, fileName);
        if (!file.exists()) {
            file.createNewFile();
        }
        return file;
    }
    
    public static File getTestDir(String testName) {
        File dir = new File(TEST_OUTPUT_DIR, testName);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return dir;
    }
    
    public static void cleanDirectory(File dir) throws IOException {
        File[] files = dir.listFiles();

        for (File f : files) {
            if (f.isFile()) {
                boolean deleted = f.delete();
                if (!deleted) {
                    throw new IOException("file:" + f.getAbsolutePath() + " not deleted");
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
                    throw new IOException("file:" + f.getAbsolutePath() + " not deleted");
                }
            }
        }

        boolean deleted = dir.delete();
        if (!deleted) {
            throw new IOException("dir:" + dir.getAbsolutePath() + " not deleted");
        }
    }
}
