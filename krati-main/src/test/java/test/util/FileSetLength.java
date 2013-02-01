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
