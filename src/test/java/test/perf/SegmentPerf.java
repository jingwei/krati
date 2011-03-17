package test.perf;

import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;

import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.Segment;
import krati.core.segment.SegmentFactory;
import krati.core.segment.SegmentManager;

public class SegmentPerf {
    static Random random = new Random(System.currentTimeMillis());
    
    final int maxDataBytes = 2048;
    final SegmentManager segManager;
    final int[] sArray;       // size
    final int[] pArray;       // position in sequential order
    final int[] pArrayRandom; // position in random order
    final byte[][] bArray;    // array of byte array
    final int datLength;
    
    public SegmentPerf(SegmentFactory segFactory, String segmentHomePath, int initSizeMB) throws IOException {
        segManager = SegmentManager.getInstance(segmentHomePath, segFactory, initSizeMB);
        
        long initSizeBytes = initSizeMB * 1024L * 1024L;
        datLength = (int)(initSizeBytes / maxDataBytes);
        
        pArray = new int[datLength];
        sArray = new int[datLength];
        
        pArray[0] = 0;
        sArray[0] = random.nextInt(maxDataBytes);
        for (int i = 1; i < datLength; i++) {
            pArray[i] = pArray[i-1] + sArray[i-1];
            sArray[i] = random.nextInt(maxDataBytes);
        }

        bArray = new byte[datLength][];
        for (int i = 0; i < datLength; i++) {
            bArray[i] = new byte[sArray[i]];
        }
        
        pArrayRandom = new int[datLength];
        for (int i = 0; i < datLength; i++) {
            pArrayRandom[i] = random.nextInt((int)(initSizeBytes-1));
        }
        
        int ind = 0;
        for (int i = 0; i < bArray[ind].length; i++) {
            bArray[ind][i] = 'A';
        }
        
        ind = 10000;
        for (int i = 0; i < bArray[ind].length; i++) {
            bArray[ind][i] = 'B';
        }
        
        ind = bArray.length-1;
        for (int i = 0; i < bArray[ind].length; i++) {
            bArray[ind][i] = 'C';
        }
    }
    
    static class SegmentReader implements Runnable {
        Set<SegmentReader> runnerSet;
        Segment segment;
        int[] pArray;
        byte[][] bArray;
        
        public SegmentReader(Set<SegmentReader> runnerSet, Segment segment, int[] pArray, byte[][] bArray) {
            this.segment = segment;
            this.pArray = pArray;
            this.bArray = bArray;
            this.runnerSet = runnerSet;
            this.runnerSet.add(this);
        }
        
        @Override
        public void run() {
            long startTime, endTime, diffTime;
            
            try {
                startTime = System.currentTimeMillis();
                for (int i = 0, cnt = pArray.length; i < cnt; i++) {
                    try {
                        segment.read(pArray[i], bArray[i]);
                    } catch (Exception e) {
                        System.err.println(segment.getSegmentId() + " : " + e.getMessage());
                    }
                }
                endTime = System.currentTimeMillis();

                diffTime = endTime - startTime;
                System.out.printf("Segment %d finished: %d reads in %d ms, avg %7.5f ms%n",
                                  segment.getSegmentId(), pArray.length, diffTime, ((float) diffTime / pArray.length));
            } finally {
                this.runnerSet.remove(this);
            }
        }   
    }
    
    private void printReadData(Segment segment, int index) throws IOException {
        byte[] b = new byte[bArray[index].length];

        System.out.println();
        segment.read(pArray[index], b);
        for (int i = 0; i < b.length; i++) {
            System.out.print((char)b[i]);
        }
        System.out.println();
    }
    
    public int getSegmentCount() {
        return segManager.getSegmentCount();
    }
    
    public Segment getSegment(int segId) {
        if (!(segId < segManager.getSegmentCount())) {
            for (int i = 0; i <= segId - segManager.getSegmentCount(); i++) {
                try {
                    segManager.nextSegment();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        }

        return segManager.getSegment(segId);
    }
    
    public void testAppend(Segment segment) {
        long startTime, endTime, diffTime;

        try {
            startTime = System.currentTimeMillis();
            for (byte[] b : bArray) {
                segment.append(b);
            }
            endTime = System.currentTimeMillis();

            diffTime = endTime - startTime;
            System.out.printf("Append: %d ms, avg %7.5f ms%n", diffTime, ((float) diffTime / datLength));
            System.out.println();

            printReadData(segment, 0);
            printReadData(segment, 10000);
            printReadData(segment, bArray.length - 1);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }
    
    public void testRead(Segment segment) throws IOException {
        long startTime, endTime, diffTime;

        startTime = System.currentTimeMillis();
        for (int i = 0; i < datLength; i++) {
            try {
                segment.read(pArrayRandom[i], bArray[i]);
            } catch (IndexOutOfBoundsException e) {
                // do nothing
            }
        }
        endTime = System.currentTimeMillis();

        diffTime = endTime - startTime;
        System.out.printf("Read: %d ms, avg %7.5f ms%n", diffTime, ((float) diffTime / datLength));
        System.out.println();

        printReadData(segment, 0);
        printReadData(segment, 10000);
        printReadData(segment, bArray.length - 1);
    }
    
    public void testReadMultiSegments(int segCount) {
        long startTime, endTime, diffTime;
        System.out.printf("Read multi-segments (%d)%n", segCount);

        try {
            while (segManager.getSegmentCount() < segCount) {
                Segment segment = segManager.nextSegment();
                testAppend(segment);
            }

            Set<SegmentReader> runnerSet = new HashSet<SegmentReader>();
            SegmentReader[] runnerArray = new SegmentReader[segManager.getSegmentCount()];
            for (int i = 0; i < runnerArray.length; i++) {
                runnerArray[i] = new SegmentReader(runnerSet, segManager.getSegment(i), pArrayRandom, bArray);
                System.out.printf("SegmentReader %d%n", segManager.getSegment(i).getSegmentId());
            }

            startTime = System.currentTimeMillis();
            for (SegmentReader r : runnerArray) {
                r.run();
            }
            endTime = System.currentTimeMillis();

            diffTime = endTime - startTime;
            System.out.printf("Read: %d ms, avg %7.5f ms%n", diffTime, ((float) diffTime / datLength / segManager.getSegmentCount()));
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
    
    public void testReadMultiSegmentsWithThreads(int segCount) {
        long startTime, endTime, diffTime;
        System.out.printf("Read multi-segment (%d) with threads%n", segCount);
        
        try {
            while(segManager.getSegmentCount() < segCount) {
                Segment segment = segManager.nextSegment();
                testAppend(segment);
            }
            
            Set<SegmentReader> runnerSet = new HashSet<SegmentReader>();
            SegmentReader[] runnerArray = new SegmentReader[segManager.getSegmentCount()];
            for (int i = 0; i < runnerArray.length; i++) {
                runnerArray[i] = new SegmentReader(runnerSet, segManager.getSegment(i), pArrayRandom, bArray);
                System.out.printf("SegmentReader %d%n", segManager.getSegment(i).getSegmentId());
            }
            
            startTime = System.currentTimeMillis();
            for (SegmentReader r : runnerArray) {
                new Thread(r).start();
            }
            
            while (runnerSet.size() > 0) {
                Thread.sleep(10);
            }
            endTime = System.currentTimeMillis();
            
            diffTime = endTime - startTime;
            System.out.printf("Read: %d ms, avg %7.5f ms%n", diffTime, ((float)diffTime/datLength/segManager.getSegmentCount()));
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        final int initSizeMB = 2048;
        
        try {
            SegmentPerf sp = new SegmentPerf(new MappedSegmentFactory(), "test/test_data/segs", initSizeMB);
            if (sp.getSegmentCount() == 0) {
                sp.testAppend(sp.getSegment(0));
                sp.testRead(sp.getSegment(0));
            }

            int segCount = 8;
            // sp.testReadMultiSegments(segCount);
            sp.testReadMultiSegmentsWithThreads(segCount);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            System.out.println("TEST DONE");
        }
    }
}
