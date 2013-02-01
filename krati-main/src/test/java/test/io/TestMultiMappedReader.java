package test.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import junit.framework.TestCase;
import krati.io.MultiMappedReader;
import test.util.FileSetLength;
import test.util.FileUtils;

/**
 * TestMultiMappedReader
 * 
 * @author jwu
 * @since 01/31, 2013
 */
public class TestMultiMappedReader extends TestCase {
    protected File file;
    protected MultiMappedReader reader;
    protected Random rand = new Random();

    @Override
    protected void setUp() {
        try {
            file = FileUtils.getTestFile(getClass().getSimpleName() + ".dat");
            reader = new MultiMappedReader(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void tearDown() {
        try {
            reader.close();
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

        reader.open();
        writeRandomValues();

        assertEquals(fileLength, file.length());
        assertEquals(fileLength, reader.getMappedLength());

        fileLength += rand.nextInt(Integer.MAX_VALUE);
        raf.setLength(fileLength);

        reader.remap();
        writeRandomValues();

        assertEquals(fileLength, file.length());
        assertEquals(fileLength, reader.getMappedLength());

        for (int i = 0, cnt = rand.nextInt(10); i < cnt; i++) {
            if (rand.nextFloat() < 0.75f) {
                fileLength += rand.nextInt(MultiMappedReader.BUFFER_SIZE);
            } else {
                fileLength -= rand.nextInt(MultiMappedReader.BUFFER_SIZE);
            }
            raf.setLength(fileLength);

            reader.remap();
            writeRandomValues();

            assertEquals(fileLength, file.length());
            assertEquals(fileLength, reader.getMappedLength());
        }
    }

    public void testRemapThread() throws Exception {
        int length1 = Integer.MAX_VALUE;
        int length2 = rand.nextInt(Integer.MAX_VALUE);

        long fileLength = 0L;
        fileLength += length1;
        fileLength += length2;
        setFileLength(fileLength);

        reader.open();
        writeRandomValues();

        assertEquals(fileLength, file.length());
        assertEquals(fileLength, reader.getMappedLength());

        fileLength += rand.nextInt(Integer.MAX_VALUE);
        setFileLength(fileLength);

        reader.remap();
        writeRandomValues();

        assertEquals(fileLength, file.length());
        assertEquals(fileLength, reader.getMappedLength());

        for (int i = 0, cnt = rand.nextInt(10); i < cnt; i++) {
            if (rand.nextFloat() < 0.75f) {
                fileLength += rand.nextInt(MultiMappedReader.BUFFER_SIZE);
            } else {
                fileLength -= rand.nextInt(MultiMappedReader.BUFFER_SIZE);
            }
            setFileLength(fileLength);

            reader.remap();
            writeRandomValues();

            assertEquals(fileLength, file.length());
            assertEquals(fileLength, reader.getMappedLength());
        }
    }

    void setFileLength(long length) throws InterruptedException {
        Thread t = new Thread(new FileSetLength(file, length));
        t.run();
        t.join();
    }

    void writeRandomValues() throws IOException {
        long position = reader.getMappedLength() - 4;
        for (int i = 0; i < 100; i++) {
            position -= rand.nextInt(MultiMappedReader.BUFFER_SIZE);
            if (position >= 0) {
                reader.readInt(position);
            } else {
                break;
            }
        }
    }
}
