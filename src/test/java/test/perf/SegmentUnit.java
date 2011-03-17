package test.perf;

import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

import java.nio.ByteBuffer;
import java.nio.BufferOverflowException;
import java.nio.channels.FileChannel;
import java.util.Random;

import org.apache.log4j.Logger;

public class SegmentUnit {
    private final static Logger log = Logger.getLogger(SegmentUnit.class);
    private final File segFile;
    private final int initSizeMB;
    private final boolean memMapped;
    
    private ByteBuffer byteBuffer;
    private RandomAccessFile rafRead;
    
    public SegmentUnit(File segmentFile, int initialSizeMB, boolean memoryMapped) throws IOException {
        this.segFile = segmentFile;
        this.initSizeMB = initialSizeMB;
        this.memMapped = memoryMapped;
        this.init();
    }
    
    public File getSegmentFile() {
        return segFile;
    }
    
    public int getInitialSize() {
        return initSizeMB * 1024 * 1024;
    }
    
    public int getInitialSizeMB() {
        return initSizeMB;
    }
    
    public boolean isMemoryMapped() {
        return memMapped;
    }
    
    protected void init() throws FileNotFoundException, IOException {
        int initSizeBytes = getInitialSize();
        
        if (!getSegmentFile().exists()) {
            if (!getSegmentFile().createNewFile()) {
                String msg = "Failed to create " + getSegmentFile().getAbsolutePath();

                log.error(msg);
                throw new IOException(msg);
            }
        }
        
        if (getSegmentFile().exists()) {
            RandomAccessFile raf = new RandomAccessFile(getSegmentFile(), "rw");
            if (raf.length() < initSizeBytes) {
                raf.setLength(initSizeBytes);
            }
        }
        
        if (memMapped) {
            // Create MappedByteBuffer
            RandomAccessFile raf = new RandomAccessFile(getSegmentFile(), "rw");
            byteBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, initSizeBytes);
        } else {
            byteBuffer = ByteBuffer.allocate(initSizeBytes);
        }
        
        rafRead = new RandomAccessFile(getSegmentFile(), "r");
    }
    
    public void append(byte[] data) throws BufferOverflowException {
        byteBuffer.put(data, 0, data.length);
    }
    
    public void read(int pos, byte[] dst) {
        if (memMapped) {
            for (int i = 0; i < dst.length; i++) {
                dst[i] = byteBuffer.get(pos + i);
            }
        } else {
            byte[] array = byteBuffer.array();
            System.arraycopy(array, pos, dst, 0, dst.length);
        }
    }
    
    public void rafRead(long pos, byte[] dst) throws IOException {
        rafRead.seek(pos);
        rafRead.read(dst);
    }
    
    static Random random = new Random(System.currentTimeMillis());
    
    public static void main(String[] args) {
        final int initSizeMB = 1024;
        final int initSizeBytes = initSizeMB * 1024 * 1024;
        final int POS = 0;
        final int SIZE = 1;
        final int maxDataBytes = 2048;
        final int datLength = (int)(initSizeMB * 1024 * 1024 / maxDataBytes);
        int[][] datArray = new int[datLength][2];
        
        long startTime, endTime, diffTime;
        
        datArray[0][POS] = 0;
        datArray[0][SIZE] = random.nextInt(maxDataBytes);
        
        for (int i = 1; i < datLength; i++) {
            datArray[i][POS] = datArray[i-1][POS] + datArray[i-1][SIZE];
            datArray[i][SIZE] = random.nextInt(maxDataBytes);
        }
        
        byte[][] bArray = new byte[datLength][];
        for (int i = 0; i < datLength; i++) {
            int[] dat = datArray[i]; 
            bArray[i] = new byte[dat[SIZE]];
        }
        
        SegmentUnit segment;
        
        System.out.println("APPEND");
        
        try {
            startTime = System.currentTimeMillis();
            segment = new SegmentUnit(new File("test/test_data/segment"), initSizeMB, false);
            endTime = System.currentTimeMillis();
            
            diffTime = endTime - startTime;
            System.out.printf("mmap=false init: %d ms%n", diffTime);
            
            startTime = System.currentTimeMillis();
            for (byte[] b : bArray) {
                segment.append(b);
            }
            endTime = System.currentTimeMillis();
            
            diffTime = endTime - startTime;
            System.out.printf("mmap=false append: %d ms avg %7.5f ms%n", diffTime, ((float)diffTime/datLength));
            System.out.println();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        
        try {
            startTime = System.currentTimeMillis();
            segment = new SegmentUnit(new File("test/test_data/segment"), initSizeMB, true);
            endTime = System.currentTimeMillis();
            
            diffTime = endTime - startTime;
            System.out.printf("mmap=true init: %d ms%n", diffTime);
            
            startTime = System.currentTimeMillis();
            for (byte[] b : bArray) {
                segment.append(b);
            }
            endTime = System.currentTimeMillis();
            
            diffTime = endTime - startTime;
            System.out.printf("mmap=true append: %d ms avg %7.5f ms%n", diffTime, ((float)diffTime/datLength));
            System.out.println();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        
        int[] posArrayRandom = new int[datLength];
        for (int i = 0; i < datLength; i++) {
            posArrayRandom[i] = random.nextInt(initSizeBytes);
        }
        
        System.out.println("READ");
        try {
            startTime = System.currentTimeMillis();
            segment = new SegmentUnit(new File("test/test_data/segment"), initSizeMB, false);
            endTime = System.currentTimeMillis();
            
            System.out.printf("mmap=false init: %d ms%n", endTime - startTime);
            
            startTime = System.currentTimeMillis();
            for (int i = 0; i < datLength; i++) {
                try {
                    segment.read(posArrayRandom[i], bArray[i]);
                } catch (IndexOutOfBoundsException e) {
                    // do nothing
                }
            }
            endTime = System.currentTimeMillis();

            diffTime = endTime - startTime;
            System.out.printf("mmap=false read: %d ms avg %7.5f ms%n", diffTime, ((float)diffTime/datLength));
            System.out.println();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        
        try {
            startTime = System.currentTimeMillis();
            segment = new SegmentUnit(new File("test/test_data/segment"), initSizeMB, true);
            endTime = System.currentTimeMillis();
            
            System.out.printf("mmap=true init: %d ms%n", endTime - startTime);
            
            startTime = System.currentTimeMillis();
            for (int i = 0; i < datLength; i++) {
                try {
                    segment.read(posArrayRandom[i], bArray[i]);
                } catch (IndexOutOfBoundsException e) {
                    // do nothing
                }
            }
            endTime = System.currentTimeMillis();
            
            diffTime = endTime - startTime;
            System.out.printf("mmap=true mapRead: %d ms avg %7.5f ms%n", diffTime, ((float)diffTime/datLength));
            System.out.println();
            
            startTime = System.currentTimeMillis();
            for (int i = 0; i < datLength; i++) {
                try {
                    segment.rafRead(posArrayRandom[i], bArray[i]);
                } catch (IndexOutOfBoundsException e) {
                    // do nothing
                }
            }
            endTime = System.currentTimeMillis();
            
            diffTime = endTime - startTime;
            System.out.printf("mmap=true rafRead: %d ms avg %7.5f ms%n", diffTime, ((float)diffTime/datLength));
            System.out.println();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
    
}
