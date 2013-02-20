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
import java.io.RandomAccessFile;

/**
 * FileSetLength
 * 
 * @author jwu
 * @since 01/31, 2013
 */
public class FileSetLength implements Runnable {
    private final File file;
    private final long length;

    public FileSetLength(File file, long length) {
        this.file = file;
        this.length = length;
    }

    @Override
    public void run() {
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.setLength(length);
            raf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
