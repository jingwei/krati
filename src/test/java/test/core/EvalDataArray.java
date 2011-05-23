package test.core;

import java.io.File;
import java.io.IOException;

import krati.array.DataArray;
import krati.core.segment.SegmentFactory;
import krati.store.AbstractDataArray;
import test.AbstractTest;
import test.StatsLog;
import test.util.DataArrayChecker;
import test.util.DataArrayReader;
import test.util.DataArrayWriter;

/**
 * EvalDataArray
 * 
 * @author jwu
 * 
 */
public abstract class EvalDataArray extends AbstractTest {
    
    public EvalDataArray() {
        super(EvalDataArray.class.getName());
    }
    
    public void populate(DataArray dataArray) throws IOException {
        String line;
        int index = 0;
        int length = dataArray.length();
        int lineCnt = _lineSeedData.size();
        
        long startTime = System.currentTimeMillis();
        
        while(index < length) {
            try {
                line = _lineSeedData.get(index % lineCnt);
                dataArray.set(index, line.getBytes(), System.currentTimeMillis());
            } catch(Exception e) {
                e.printStackTrace();
            }
            index++;
        }
        
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
        
        double rate = dataArray.length()/(double)elapsedTime;
        rate = Math.round(rate * 100) / 100.0;
        StatsLog.logger.info("writeCount="+ dataArray.length() +" rate="+ rate +" per ms");
    }
    
    public static void checkData(DataArray dataArray, int index) {
        String line = _lineSeedData.get(index % _lineSeedData.size());
        
        byte[] b = dataArray.get(index);
        if (b != null) {
            String s = new String(b);
            assertTrue("[" + index + "]=" + s + " expected=" + line, s.equals(line));
        } else {
            assertTrue("[" + index + "]=null", line == null);
        }
    }
    
    public static void validate(DataArray dataArray) {
        long startTime = System.currentTimeMillis();
        
        int len = dataArray.length();
        for(int i = 0; i < len; i++) {
            checkData(dataArray, i);
        }
        StatsLog.logger.info("OK");
        
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
        
        double rate = dataArray.length()/(double)elapsedTime;
        rate = Math.round(rate * 100) / 100.0;
        StatsLog.logger.info("validateCount="+ dataArray.length() +" rate="+ rate +" per ms");
    }
    
    public static double evalWrite(DataArray dataArray, int runDuration) throws Exception {
        try {
            // Start writer
            DataArrayWriter writer = new DataArrayWriter(dataArray, _lineSeedData);
            Thread writerThread = new Thread(writer);
            writerThread.start();
            StatsLog.logger.info("Writer started");
            
            long startTime = System.currentTimeMillis();
            long writeCount = 0;
            int heartBeats = runDuration/10;
            long sleepTime = Math.min(runDuration * 1000, 10000);
            for(int i = 0; i < heartBeats; i++) {
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
        }
        catch(Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    public static void evalRead(DataArray dataArray, int readerCnt, int runDuration) throws Exception {
        try {
            // Start readers
            DataArrayReader[] readers = new DataArrayReader[readerCnt];
            for(int i = 0; i < readers.length; i++) {
                readers[i] = new DataArrayReader(dataArray, _lineSeedData);
            }
            
            Thread[] threads = new Thread[readers.length];
            for(int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(readers[i]);
                threads[i].start();
                StatsLog.logger.info("Reader " + i + " started");
            }
            
            long startTime = System.currentTimeMillis();
            
            // Sleep until run time is exhausted
            Thread.sleep(runDuration * 1000);
            
            for(int i = 0; i < readers.length; i++) {
                readers[i].stop();
            }
            for(int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
            
            long endTime = System.currentTimeMillis();
            
            double sumReadRate = 0;
            long elapsedTime = endTime - startTime;
            StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
            for(int i = 0; i < readers.length; i++) {
                double rate = readers[i].getReadCount()/(double)elapsedTime;
                rate = Math.round(rate * 100) / 100.0;
                StatsLog.logger.info("readCount["+ i +"]="+ readers[i].getReadCount() +" rate="+ rate +" per ms");
                sumReadRate += rate;
            }
            
            sumReadRate = Math.round(sumReadRate * 100) / 100.0;
            StatsLog.logger.info("Total Read Rate="+ sumReadRate +" per ms");
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    public static void evalReadWrite(DataArray dataArray, int readerCnt, int runDuration, boolean doValidation) throws Exception {
        try {
            // Start readers
            DataArrayReader[] readers = new DataArrayReader[readerCnt];
            for(int i = 0; i < readers.length; i++) {
                readers[i] = doValidation ? new DataArrayChecker(dataArray, _lineSeedData) : new DataArrayReader(dataArray, _lineSeedData);
            }

            Thread[] threads = new Thread[readers.length];
            for(int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(readers[i]);
                threads[i].start();
                StatsLog.logger.info("Reader " + i + " started");
            }
            
            // Start writer
            DataArrayWriter writer = new DataArrayWriter(dataArray, _lineSeedData);
            Thread writerThread = new Thread(writer);
            writerThread.start();
            StatsLog.logger.info("Writer started");
            
            long startTime = System.currentTimeMillis();
            
            long readCount = 0;
            long writeCount = 0;
            int heartBeats = runDuration/10;
            long sleepTime = Math.min(runDuration * 1000, 10000);
            for(int i = 0; i < heartBeats; i++) {
                Thread.sleep(sleepTime);

                long newReadCount = 0;
                for(int r = 0; r < readers.length; r++) {
                    newReadCount += readers[r].getReadCount();
                }
                
                long newWriteCount = writer.getWriteCount();
                
                StatsLog.logger.info("write="+ (newWriteCount-writeCount) +" read=" + (newReadCount-readCount));
                
                readCount = newReadCount;
                writeCount = newWriteCount;
            }
            
            // Stop reader
            for(int i = 0; i < readers.length; i++) {
                readers[i].stop();
            }
            for(int i = 0; i < threads.length; i++) {
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
            for(int i = 0; i < readers.length; i++) {
                rate = readers[i].getReadCount()/(double)elapsedTime;
                rate = Math.round(rate * 100) / 100.0;
                StatsLog.logger.info("readCount["+ i +"]="+ readers[i].getReadCount() +" rate="+ rate +" per ms");
                sumReadRate += rate;
            }
            
            sumReadRate = Math.round(sumReadRate * 100) / 100.0;
            StatsLog.logger.info("Total Read Rate="+ sumReadRate +" per ms");
            
            StatsLog.logger.info("writer latency stats:");
            writer.getLatencyStats().print(StatsLog.logger);
            
            if(!doValidation) {
                StatsLog.logger.info("reader latency stats:");
                readers[0].getLatencyStats().print(StatsLog.logger);
            }
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    protected abstract SegmentFactory createSegmentFactory();
    
    protected abstract AbstractDataArray createDataArray(File homeDir) throws Exception;
        
    public void testDataArray() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        try {
            File homeDir = getHomeDirectory();
            AbstractDataArray dataArray = createDataArray(homeDir);
            
            if (dataArray.getLWMark() == 0) {
                StatsLog.logger.info(">>> populate");
                populate(dataArray);
                
                StatsLog.logger.info(">>> validate");
                validate(dataArray);
            }
            
            int timeAllocated = Math.round((float)_runTimeSeconds/3);

            StatsLog.logger.info(">>> read only");
            evalRead(dataArray, _numReaders, Math.min(timeAllocated, 10));
            
            StatsLog.logger.info(">>> write only");
            evalWrite(dataArray, timeAllocated);
            dataArray.persist();
            
            StatsLog.logger.info(">>> validate");
            validate(dataArray);
            
            StatsLog.logger.info(">>> read & write");
            evalReadWrite(dataArray, _numReaders, timeAllocated, false);
            dataArray.persist();
            
            StatsLog.logger.info(">>> validate");
            validate(dataArray);
            
            StatsLog.logger.info(">>> check & write");
            evalReadWrite(dataArray, _numReaders, timeAllocated, true);
            dataArray.persist();
            
            StatsLog.logger.info(">>> validate");
            validate(dataArray);
            
            dataArray.sync();
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
