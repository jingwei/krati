package test.util;

import java.io.File;
import java.io.IOException;

/**
 * DirUtils
 * 
 * @author jwu
 * 
 * <p>
 * 08/09, 2011 - Created <br/>
 */
public final class DirUtils {
    
    public static File getTestDir(Class<?> testClass) {
        File dir = new File(System.getProperty("krati.test.output.dir"), testClass.getSimpleName());
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return dir;
    }
    
    public static void cleanDirectory(File dir) throws IOException {
        File[] files = dir.listFiles();
        if(files != null) {
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
    }

    public static void deleteDirectory(File dir) throws IOException {
        if(!dir.exists()) {
            return;
        }
        
        File[] files = dir.listFiles();
        if(files != null) {
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
        }
        
        boolean deleted = dir.delete();
        if (!deleted) {
            throw new IOException("dir:" + dir.getAbsolutePath() + " not deleted");
        }
    }
}
