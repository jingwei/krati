package test.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import test.util.FileSetLength;
import test.util.FileUtils;

import junit.framework.TestCase;
import krati.io.MultiMappedWriter;

/**
 * TestMultiMappedWriter
 * 
 * @author jwu
 * @since 01/31, 2013
 */
public class TestMultiMappedWriter extends TestCase {
    protected File file;
    protected MultiMappedWriter writer;
    protected Random rand = new Random();

    @Override
    protected void setUp() {
        try {
            file = FileUtils.getTestFile(getClass().getSimpleName() + ".dat");
            writer = new MultiMappedWriter(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void tearDown() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (file != null && file.exists()) {
            file.delete();
        }
    }

    public void testRemap() throws IOException {
        int length1 = Integer.MAX_VALUE;
        int length2 = rand.nextInt(Integer.MAX_VALUE);
        RandomAccessFile raf = new RandomAccessFile(file, "rw");

        long fileLength = 0L;
        fileLength += length1;
        fileLength += length2;
        raf.setLength(fileLength);

        writer.open();
        writeRandomValues();

        assertEquals(fileLength, file.length());
        assertEquals(fileLength, writer.getMappedLength());

        fileLength += rand.nextInt(Integer.MAX_VALUE);
        raf.setLength(fileLength);

        writer.remap();
        writeRandomValues();

        assertEquals(fileLength, file.length());
        assertEquals(fileLength, writer.getMappedLength());

        for (int i = 0, cnt = rand.nextInt(10); i < cnt; i++) {
            if (rand.nextFloat() < 0.75f) {
                fileLength += rand.nextInt(MultiMappedWriter.BUFFER_SIZE);
            } else {
                fileLength -= rand.nextInt(MultiMappedWriter.BUFFER_SIZE);
            }
            raf.setLength(fileLength);

            writer.remap();
            writeRandomValues();

            assertEquals(fileLength, file.length());
            assertEquals(fileLength, writer.getMappedLength());
        }
    }

    public void testRemapThread() throws Exception {
        int length1 = Integer.MAX_VALUE;
        int length2 = rand.nextInt(Integer.MAX_VALUE);

        long fileLength = 0L;
        fileLength += length1;
        fileLength += length2;
        setFileLength(fileLength);

        writer.open();
        writeRandomValues();

        assertEquals(fileLength, file.length());
        assertEquals(fileLength, writer.getMappedLength());

        fileLength += rand.nextInt(Integer.MAX_VALUE);
        setFileLength(fileLength);

        writer.remap();
        writeRandomValues();

        assertEquals(fileLength, file.length());
        assertEquals(fileLength, writer.getMappedLength());

        for (int i = 0, cnt = rand.nextInt(10); i < cnt; i++) {
            if (rand.nextFloat() < 0.75f) {
                fileLength += rand.nextInt(MultiMappedWriter.BUFFER_SIZE);
            } else {
                fileLength -= rand.nextInt(MultiMappedWriter.BUFFER_SIZE);
            }
            setFileLength(fileLength);

            writer.remap();
            writeRandomValues();

            assertEquals(fileLength, file.length());
            assertEquals(fileLength, writer.getMappedLength());
        }
    }

    void setFileLength(long length) throws InterruptedException {
        Thread t = new Thread(new FileSetLength(file, length));
        t.run();
        t.join();
    }

    void writeRandomValues() throws IOException {
        int value;
        long position = writer.getMappedLength() - 4;
        for (int i = 0; i < 100; i++) {
            position -= rand.nextInt(MultiMappedWriter.BUFFER_SIZE);
            if (position >= 0) {
                value = rand.nextInt();
                writer.writeInt(position, value);
            } else {
                break;
            }
        }
    }
}
