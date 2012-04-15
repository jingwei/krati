/*
 * Copyright (c) 2010-2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package test.util;

import java.io.File;
import java.io.IOException;

/**
 * DirUtils
 * 
 * @author jwu
 * @since 08/09, 2011
 */
public final class DirUtils {
    
    public static File getTestDir(Class<?> testClass) {
        String path = System.getProperty("krati.avro.test.dir");
        if(path == null) {
            path = System.getProperty("java.io.tmpdir");
        }
        
        File dir = new File(path, testClass.getSimpleName());
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
