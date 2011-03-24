package test.core;

import java.io.File;
import java.io.IOException;

import krati.core.segment.SegmentFactory;
import krati.store.ArrayStorePartition;
import krati.store.StaticArrayStorePartition;

import test.AbstractSeedTest;
import test.StatsLog;
import test.util.DataPartitionChecker;
import test.util.DataPartitionReader;
import test.util.DataPartitionWriter;

/**
 * TestDataPartition using MemorySegment 
 * 
 * @author jwu
 *
 */
public class TestDataPartition extends AbstractSeedTest {
    
    public TestDataPartition() {
        super(TestDataPartition.class.getName());
    }
    
    public void populate(ArrayStorePartition partition) throws IOException {
        String line;
        int lineCnt = _lineSeedData.size();
        int index = partition.getIdStart();
        int stopIndex = index + partition.getIdCount();
        
        long scn = partition.getHWMark();
        long startTime = System.currentTimeMillis();
        
        while (index < stopIndex) {
            try {
                line = _lineSeedData.get(index % lineCnt);
                partition.set(index, line.getBytes(), scn++);
            } catch (Exception e) {
                e.printStackTrace();
            }
            index++;
        }
        
        partition.persist();

        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
        
        double rate = partition.getIdCount()/(double)elapsedTime;
        rate = Math.round(rate * 100) / 100.0;
        StatsLog.logger.info("writeCount="+ partition.getIdCount() +" rate="+ rate +" per ms");
    }
    
    public static void checkData(ArrayStorePartition partition, int index) {
        String line = _lineSeedData.get(index % _lineSeedData.size());
        
        byte[] b = partition.get(index);
        if (b != null) {
            String s = new String(b);
            assertTrue("[" + index + "]=" + s + " expected=" + line, s.equals(line));
        } else {
            assertTrue("[" + index + "]=null", line == null);
        }
    }
    
    public static void validate(ArrayStorePartition partition) {
        int size = partition.getIdCount();
        for (int i = 0; i < size; i++) {
            int index = partition.getIdStart() + i;
            checkData(partition, index);
        }
        StatsLog.logger.info("OK");
    }
    
    public static double evalWrite(ArrayStorePartition partition, int runDuration) throws Exception {
        try {
            // Start writer
            DataPartitionWriter writer = new DataPartitionWriter(partition, _lineSeedData);
            Thread writerThread = new Thread(writer);
            writerThread.start();
            StatsLog.logger.info("Writer started");
            
            long startTime = System.currentTimeMillis();
            long writeCount = 0;
            int heartBeats = runDuration/10;
            long sleepTime = Math.min(runDuration * 1000, 10000);
            for (int i = 0; i < heartBeats; i++) {
                Thread.sleep(sleepTime);
                long newWriteCount = writer.getWriteCount();
                StatsLog.logger.info("writeCount=" + (newWriteCount - writeCount));
                writeCount = newWriteCount;
            }
            
            writer.stop();
            writerThread.join();
            
            long endTime = System.currentTimeMillis();

            long elapsedTime = endTime - startTime;
            StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
            
            double rate = writer.getWriteCount()/(double)elapsedTime;
            rate = Math.round(rate * 100) / 100.0;
            StatsLog.logger.info("writeCount="+ writer.getWriteCount() +" rate="+ rate +" per ms");
            
            return rate;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    public static void evalRead(ArrayStorePartition partition, int readerCnt, int runDuration) throws Exception {
        try {
            // Start readers
            DataPartitionReader[] readers = new DataPartitionReader[readerCnt];
            for (int i = 0; i < readers.length; i++) {
                readers[i] = new DataPartitionReader(partition, _lineSeedData);
            }
            
            Thread[] threads = new Thread[readers.length];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(readers[i]);
                threads[i].start();
                StatsLog.logger.info("Reader " + i + " started");
            }
            
            long startTime = System.currentTimeMillis();
            
            // Sleep until run time is exhausted
            Thread.sleep(runDuration * 1000);
            
            for (int i = 0; i < readers.length; i++) {
                readers[i].stop();
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
            
            long endTime = System.currentTimeMillis();
            
            double sumReadRate = 0;
            long elapsedTime = endTime - startTime;
            StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
            for (int i = 0; i < readers.length; i++) {
                double rate = readers[i].getReadCount()/(double)elapsedTime;
                rate = Math.round(rate * 100) / 100.0;
                StatsLog.logger.info("readCount["+ i +"]="+ readers[i].getReadCount() +" rate="+ rate +" per ms");
                sumReadRate += rate;
            }
            
            sumReadRate = Math.round(sumReadRate * 100) / 100.0;
            StatsLog.logger.info("Total Read Rate="+ sumReadRate +" per ms");
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    
    public static void evalReadWrite(ArrayStorePartition partition, int readerCnt, int runDuration, boolean doValidation) throws Exception {
        try {
            // Start readers
            DataPartitionReader[] readers = new DataPartitionReader[readerCnt];
            for (int i = 0; i < readers.length; i++) {
                readers[i] = doValidation ? new DataPartitionChecker(partition, _lineSeedData) : new DataPartitionReader(partition, _lineSeedData);
            }

            Thread[] threads = new Thread[readers.length];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(readers[i]);
                threads[i].start();
                StatsLog.logger.info("Reader " + i + " started");
            }
            
            // Start writer
            DataPartitionWriter writer = new DataPartitionWriter(partition, _lineSeedData);
            Thread writerThread = new Thread(writer);
            writerThread.start();
            StatsLog.logger.info("Writer started");
            
            long startTime = System.currentTimeMillis();
            
            long readCount = 0;
            long writeCount = 0;
            int heartBeats = runDuration/10;
            long sleepTime = Math.min(runDuration * 1000, 10000);
            for (int i = 0; i < heartBeats; i++) {
                Thread.sleep(sleepTime);

                long newReadCount = 0;
                for (int r = 0; r < readers.length; r++) {
                    newReadCount += readers[r].getReadCount();
                }
                
                long newWriteCount = writer.getWriteCount();
                
                StatsLog.logger.info("write="+ (newWriteCount-writeCount) +" read=" + (newReadCount-readCount));
                
                readCount = newReadCount;
                writeCount = newWriteCount;
            }
            
            // Stop reader
            for (int i = 0; i < readers.length; i++) {
                readers[i].stop();
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
            
            // Stop writer
            writer.stop();
            writerThread.join();
            
            long endTime = System.currentTimeMillis();

            long elapsedTime = endTime - startTime;
            StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
            
            double rate = writer.getWriteCount()/(double)elapsedTime;
            rate = Math.round(rate * 100) / 100.0;
            StatsLog.logger.info("writeCount="+ writer.getWriteCount() +" rate="+ rate +" per ms");
            
            double sumReadRate = 0;
            for (int i = 0; i < readers.length; i++) {
                rate = readers[i].getReadCount()/(double)elapsedTime;
                rate = Math.round(rate * 100) / 100.0;
                StatsLog.logger.info("readCount["+ i +"]="+ readers[i].getReadCount() +" rate="+ rate +" per ms");
                sumReadRate += rate;
            }
            
            sumReadRate = Math.round(sumReadRate * 100) / 100.0;
            StatsLog.logger.info("Total Read Rate="+ sumReadRate +" per ms");
            
            StatsLog.logger.info("writer latency stats:");
            writer.getLatencyStats().print(StatsLog.logger);
            
            if (!doValidation) {
                StatsLog.logger.info("reader latency stats:");
                readers[0].getLatencyStats().print(StatsLog.logger);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    protected SegmentFactory getSegmentFactory() {
        return new krati.core.segment.MemorySegmentFactory();
    }
    
    protected ArrayStorePartition getDataPartition(File homeDir) throws Exception {
        ArrayStorePartition partition =
            new StaticArrayStorePartition(_idStart,
                                          _idCount,
                                          homeDir,
                                          getSegmentFactory(),
                                          _segFileSizeMB);
        return partition;
    }
    
    public void testDataParition() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        TestDataPartition eval = new TestDataPartition();
        
        try {
            AbstractSeedTest.loadSeedData();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        
        try {
            File homeDir = getHomeDirectory();
            ArrayStorePartition partition = getDataPartition(homeDir);
            
            if (partition.getLWMark() == 0) {
                StatsLog.logger.info(">>> populate");
                eval.populate(partition);

                StatsLog.logger.info(">>> validate");
                validate(partition);
            }
            
            int timeAllocated = Math.round((float)_runTimeSeconds/3);
            
            StatsLog.logger.info(">>> read only");
            evalRead(partition, _numReaders, Math.min(timeAllocated, 10));
            
            StatsLog.logger.info(">>> write only");
            evalWrite(partition, timeAllocated);
            partition.persist();
            
            StatsLog.logger.info(">>> validate");
            validate(partition);
            
            StatsLog.logger.info(">>> read & write");
            evalReadWrite(partition, _numReaders, timeAllocated, false);
            partition.persist();
            
            StatsLog.logger.info(">>> validate");
            validate(partition);
            
            StatsLog.logger.info(">>> check & write");
            evalReadWrite(partition, _numReaders, timeAllocated, true);
            partition.persist();
            
            StatsLog.logger.info(">>> validate");
            validate(partition);
            
            partition.sync();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
