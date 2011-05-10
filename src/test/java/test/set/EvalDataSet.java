package test.set;

import java.io.File;
import java.io.IOException;

import krati.core.segment.SegmentFactory;
import krati.store.DataSet;
import test.AbstractTest;
import test.StatsLog;
import test.util.DataSetChecker;
import test.util.DataSetReader;
import test.util.DataSetRunner;
import test.util.DataSetWriter;

/**
 * EvalDataSet
 * 
 * @author jwu
 * 
 */
public abstract class EvalDataSet extends AbstractTest {
    
    protected EvalDataSet(String name) {
        super(name);
    }
    
    public void populate(DataSet<byte[]> store) throws IOException {
        int lineCnt = _lineSeedData.size();
        long startTime = System.currentTimeMillis();
        
        try {
            for (int i = 0; i < _keyCount; i++) {
                String s = _lineSeedData.get(i%lineCnt);
                String k = s.substring(0, 30) + i;
                store.add(k.getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
        
        double rate = _keyCount/(double)elapsedTime;
        rate = Math.round(rate * 100) / 100.0;
        StatsLog.logger.info("writeCount="+ _keyCount +" rate="+ rate +" per ms");
    }
    
    public void validate(DataSet<byte[]> store) {
        int i = 0;
        int errCnt = 0;
        int lineCnt = _lineSeedData.size();
        long startTime = System.currentTimeMillis();
        
        try {
            for (; i < _keyCount; i++) {
                String s = _lineSeedData.get(i % lineCnt);
                String k = s.substring(0, 30) + i;
                if (!store.has(k.getBytes())) {
                    errCnt++;
                    System.err.println("validate: value=\"" + k + "\" not found");
                    if (errCnt == 10) {
                        System.err.flush();
                        throw new RuntimeException("validate: value=\"" + k + "\" not found");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
        StatsLog.logger.info("Validated "+ i + "/" + _keyCount );
        StatsLog.logger.info("OK");
    }
    
    public double evalWrite(DataSet<byte[]> store, int runDuration) throws Exception {
        try {
            // Start writer
            DataSetWriter writer = new DataSetWriter(store, _lineSeedData, _keyCount);
            Thread writerThread = new Thread(writer);
            writerThread.start();
            StatsLog.logger.info("Writer started");
            
            long startTime = System.currentTimeMillis();
            long writeCount = 0;
            int heartBeats = runDuration/10;
            long sleepTime = Math.min(runDuration * 1000, 10000);
            for (int i = 0; i < heartBeats; i++) {
                Thread.sleep(sleepTime);
                long newWriteCount = writer.getOpCount();
                StatsLog.logger.info("writeCount=" + (newWriteCount - writeCount));
                writeCount = newWriteCount;
            }
            
            writer.stop();
            writerThread.join();
            
            long endTime = System.currentTimeMillis();
            
            long elapsedTime = endTime - startTime;
            StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
            
            double rate = writer.getOpCount()/(double)elapsedTime;
            rate = Math.round(rate * 100) / 100.0;
            StatsLog.logger.info("writeCount="+ writer.getOpCount() +" rate="+ rate +" per ms");
            
            return rate;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    public void evalRead(DataSet<byte[]> store, int readerCnt, int runDuration) throws Exception {
        try {
            // Start readers
            DataSetReader[] readers = new DataSetReader[readerCnt];
            for (int i = 0; i < readers.length; i++) {
                readers[i] = new DataSetReader(store, _lineSeedData, _keyCount);
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
                double rate = readers[i].getOpCount()/(double)elapsedTime;
                rate = Math.round(rate * 100) / 100.0;
                StatsLog.logger.info("readCount["+ i +"]="+ readers[i].getOpCount() +" rate="+ rate +" per ms");
                sumReadRate += rate;
            }
            
            sumReadRate = Math.round(sumReadRate * 100) / 100.0;
            StatsLog.logger.info("Total Read Rate="+ sumReadRate +" per ms");
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    public void evalReadWrite(DataSet<byte[]> store, int readerCnt, int runDuration, boolean doValidation) throws Exception {
        try {
            // Start readers
            DataSetRunner[] readers = new DataSetRunner[readerCnt];
            for (int i = 0; i < readers.length; i++) {
                readers[i] = doValidation ? new DataSetChecker(store, _lineSeedData, _keyCount) : new DataSetReader(store, _lineSeedData, _keyCount);
            }
            
            Thread[] threads = new Thread[readers.length];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(readers[i]);
                threads[i].start();
                StatsLog.logger.info("Reader " + i + " started");
            }
            
            // Start writer
            DataSetWriter writer = new DataSetWriter(store, _lineSeedData, _keyCount);
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
                    newReadCount += readers[r].getOpCount();
                }
                
                long newWriteCount = writer.getOpCount();
                
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
            
            double rate = writer.getOpCount()/(double)elapsedTime;
            rate = Math.round(rate * 100) / 100.0;
            StatsLog.logger.info("writeCount="+ writer.getOpCount() +" rate="+ rate +" per ms");
            
            double sumReadRate = 0;
            for (int i = 0; i < readers.length; i++) {
                rate = readers[i].getOpCount()/(double)elapsedTime;
                rate = Math.round(rate * 100) / 100.0;
                StatsLog.logger.info("readCount["+ i +"]="+ readers[i].getOpCount() +" rate="+ rate +" per ms");
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
    
    protected abstract SegmentFactory getSegmentFactory();
    
    protected abstract DataSet<byte[]> getDataSet(File storeDir) throws Exception;
    
    public void evalPerformance(int numOfReaders, int numOfWriters, int runDuration) throws Exception {
        File storeDir = getHomeDirectory();
        if(!storeDir.exists()) storeDir.mkdirs();
        cleanDirectory(storeDir);
        
        DataSet<byte[]> store = getDataSet(storeDir);
        
        try {
            int timeAllocated = Math.round((float)_runTimeSeconds/3);
            
            StatsLog.logger.info(">>> populate");
            populate(store);        
            store.persist();
            
            StatsLog.logger.info(">>> read only");
            evalRead(store, _numReaders, Math.min(timeAllocated, 10));
            
            StatsLog.logger.info(">>> write only");
            evalWrite(store, timeAllocated);
            store.persist();
            
            StatsLog.logger.info(">>> validate");
            validate(store);
            
            StatsLog.logger.info(">>> read & write");
            evalReadWrite(store, _numReaders, timeAllocated, false);
            store.persist();
            
            StatsLog.logger.info(">>> validate");
            validate(store);
            
            StatsLog.logger.info(">>> check & write");
            evalReadWrite(store, _numReaders, timeAllocated, true);
            store.persist();
            
            StatsLog.logger.info(">>> validate");
            validate(store);
            
            store.sync();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
