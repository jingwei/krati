package test.driver.raw;

import java.util.Arrays;
import java.util.List;

import test.StatsLog;
import test.driver.StoreReader;
import test.driver.StoreTestDriver;
import test.driver.StoreWriter;

/**
 * Data Store Test Driver.
 *  
 * @author jwu
 *
 * @param <S> Data Store
 */
public class StoreTestBytesDriver<S> implements StoreTestDriver {
    private final S _store;
    private final StoreReader<S, byte[], byte[]> _storeReader;
    private final StoreWriter<S, byte[], byte[]> _storeWriter;
    private final List<String> _lineSeedData;
    private final int _lineSeedCount;
    private final int _keyCount;
    private final int _hitPercent;
    
    public StoreTestBytesDriver(S store,
                                StoreReader<S, byte[], byte[]> storeReader,
                                StoreWriter<S, byte[], byte[]> storeWriter,
                                List<String> lineSeedData,
                                int keyCount, int hitPercent) {
        this._store = store;
        this._storeReader = storeReader;
        this._storeWriter = storeWriter;
        this._lineSeedData = lineSeedData;
        this._lineSeedCount = lineSeedData.size();
        this._keyCount = keyCount;
        this._hitPercent = hitPercent;
    }

    public void validate() throws Exception {
        int count = 0;
        long elapsedTime = 0;
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < _keyCount; i++) {
            count++;
            String s = _lineSeedData.get(i%_lineSeedCount);
            String k = s.substring(0, 30) + i;
            byte[] key = k.getBytes();
            byte[] value = _storeReader.get(_store, key);
            
            if (value != null) {
                if (!Arrays.equals(value, s.getBytes())) {
                    System.err.printf("key=\"%s\"%n", k);
                    System.err.printf("    \"%s\"%n", s);
                    System.err.printf("    \"%s\"%n", new String(value));
                }
            } else {
                System.err.printf("validate found null for key=\"%s\"%n", k);
            }
            
            elapsedTime = System.currentTimeMillis() - startTime;
            if (elapsedTime > 60000L) {
                StatsLog.logger.info("Quit: running time is over 60000 ms");
                break;
            }
        }
        
        StatsLog.logger.info("Validated " + count + "/" + _keyCount + " in " + elapsedTime + " ms");
        StatsLog.logger.info("OK");
    }
    
    public void populate() throws Exception {
        int count = 0;
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < _keyCount; i++) {
            String s = _lineSeedData.get(i%_lineSeedCount);
            String k = s.substring(0, 30) + i;
            _storeWriter.put(_store, k.getBytes(), s.getBytes());
            count++;
        }
        
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        StatsLog.logger.info("elapsedTime=" + elapsedTime + " ms");
        
        double rate;
        rate = count/(double)elapsedTime;
        rate = Math.round(rate * 100) / 100.0;
        StatsLog.logger.info("writeCount=" + count + " rate=" + rate + " per ms");
    }
    
    @SuppressWarnings("unchecked")
    public void evalWrite(int writerCnt, int runDuration) throws Exception {
        int hitKeyCount = Math.round(_keyCount * _hitPercent / 100.0f);
        
        try {
            // Start writers
            BytesWriteDriver<S>[] writers = new BytesWriteDriver[writerCnt];
            for (int i = 0; i < writers.length; i++) {
                writers[i] = new BytesWriteDriver<S>(_store, _storeWriter, _lineSeedData, hitKeyCount);
            }

            Thread[] writerThreads = new Thread[writers.length];
            for (int i = 0; i < writerThreads.length; i++) {
                writerThreads[i] = new Thread(writers[i]);
                writerThreads[i].start();
                StatsLog.logger.info("Writer " + i + " started");
            }

            long startTime = System.currentTimeMillis();
            long writeCount = 0;
            int heartBeats = runDuration / 10;
            long sleepTime = Math.min(runDuration * 1000, 10000);
            for (int i = 0; i < heartBeats; i++) {
                Thread.sleep(sleepTime);
                long newWriteCount = 0;
                for (int r = 0; r < writers.length; r++) {
                    newWriteCount += writers[r].getWriteCount();
                }

                StatsLog.logger.info("writeCount=" + (newWriteCount - writeCount));
                writeCount = newWriteCount;
            }

            // Stop writer
            for (int i = 0; i < writers.length; i++) {
                writers[i].stop();
            }
            for (int i = 0; i < writerThreads.length; i++) {
                writerThreads[i].join();
            }

            long endTime = System.currentTimeMillis();

            long elapsedTime = endTime - startTime;
            StatsLog.logger.info("elapsedTime=" + elapsedTime + " ms");

            double sumWriteRate = 0;
            for (int i = 0; i < writers.length; i++) {
                double rate = writers[i].getWriteCount() / (double) elapsedTime;
                rate = Math.round(rate * 100) / 100.0;
                StatsLog.logger.info("writeCount[" + i + "]=" + writers[i].getWriteCount() + " rate=" + rate + " per ms");
                sumWriteRate += rate;
            }
            sumWriteRate = Math.round(sumWriteRate * 100) / 100.0;
            StatsLog.logger.info("Total Write Rate=" + sumWriteRate + " per ms");
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    @SuppressWarnings("unchecked")
    public void evalRead(int readerCnt, int runDuration) throws Exception {
        int hitKeyCount = Math.round(_keyCount * _hitPercent / 100.0f);
        
        try {
            // Start readers
            BytesReadDriver<S>[] readers = new BytesReadDriver[readerCnt];
            for (int i = 0; i < readers.length; i++) {
                readers[i] = new BytesReadDriver<S>(_store, _storeReader, _lineSeedData, hitKeyCount);
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
            StatsLog.logger.info("elapsedTime=" + elapsedTime + " ms");
            for (int i = 0; i < readers.length; i++) {
                double rate = readers[i].getReadCount() / (double) elapsedTime;
                rate = Math.round(rate * 100) / 100.0;
                StatsLog.logger.info("readCount[" + i + "]=" + readers[i].getReadCount() + " rate=" + rate + " per ms");
                sumReadRate += rate;
            }

            sumReadRate = Math.round(sumReadRate * 100) / 100.0;
            StatsLog.logger.info("Total Read Rate=" + sumReadRate + " per ms");
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    @SuppressWarnings("unchecked")
    public void evalReadWrite(int readerCnt, int writerCnt, int runDuration, boolean doValidation) throws Exception {
        int hitKeyCount = Math.round(_keyCount * _hitPercent / 100.0f);
        
        try {
            // Start readers
            BytesReadDriver<S>[] readers = new BytesReadDriver[readerCnt];
            for (int i = 0; i < readers.length; i++) {
                readers[i] = doValidation ?
                        new BytesCheckDriver<S>(_store, _storeReader, _lineSeedData, hitKeyCount) :
                        new BytesReadDriver<S>(_store, _storeReader, _lineSeedData, hitKeyCount);
            }

            Thread[] readerThreads = new Thread[readers.length];
            for (int i = 0; i < readerThreads.length; i++) {
                readerThreads[i] = new Thread(readers[i]);
                readerThreads[i].start();
                StatsLog.logger.info("Reader " + i + " started");
            }

            // Start writers
            BytesWriteDriver<S>[] writers = new BytesWriteDriver[writerCnt];
            for (int i = 0; i < writers.length; i++) {
                writers[i] = new BytesWriteDriver<S>(_store, _storeWriter, _lineSeedData, hitKeyCount);
            }

            Thread[] writerThreads = new Thread[writers.length];
            for (int i = 0; i < writerThreads.length; i++) {
                writerThreads[i] = new Thread(writers[i]);
                writerThreads[i].start();
                StatsLog.logger.info("Writer " + i + " started");
            }

            long startTime = System.currentTimeMillis();

            long readCount = 0;
            long writeCount = 0;
            int heartBeats = runDuration / 10;
            long sleepTime = Math.min(runDuration * 1000, 10000);
            for (int i = 0; i < heartBeats; i++) {
                Thread.sleep(sleepTime);

                long newReadCount = 0;
                for (int r = 0; r < readers.length; r++) {
                    newReadCount += readers[r].getReadCount();
                }

                long newWriteCount = 0;
                for (int r = 0; r < writers.length; r++) {
                    newWriteCount += writers[r].getWriteCount();
                }

                StatsLog.logger.info("write=" + (newWriteCount - writeCount) + " read=" + (newReadCount - readCount));

                readCount = newReadCount;
                writeCount = newWriteCount;
            }

            // Stop reader
            for (int i = 0; i < readers.length; i++) {
                readers[i].stop();
            }
            for (int i = 0; i < readerThreads.length; i++) {
                readerThreads[i].join();
            }

            // Stop writer
            for (int i = 0; i < writers.length; i++) {
                writers[i].stop();
            }
            for (int i = 0; i < writerThreads.length; i++) {
                writerThreads[i].join();
            }

            long endTime = System.currentTimeMillis();

            long elapsedTime = endTime - startTime;
            StatsLog.logger.info("elapsedTime=" + elapsedTime + " ms");

            double sumWriteRate = 0;
            for (int i = 0; i < writers.length; i++) {
                double rate = writers[i].getWriteCount() / (double) elapsedTime;
                rate = Math.round(rate * 100) / 100.0;
                StatsLog.logger.info("writeCount[" + i + "]=" + writers[i].getWriteCount() + " rate=" + rate + " per ms");
                sumWriteRate += rate;
            }

            sumWriteRate = Math.round(sumWriteRate * 100) / 100.0;
            StatsLog.logger.info("Total Write Rate=" + sumWriteRate + " per ms");

            double sumReadRate = 0;
            for (int i = 0; i < readers.length; i++) {
                double rate = readers[i].getReadCount() / (double) elapsedTime;
                rate = Math.round(rate * 100) / 100.0;
                StatsLog.logger.info("readCount[" + i + "]=" + readers[i].getReadCount() + " rate=" + rate);
                sumReadRate += rate;
            }

            sumReadRate = Math.round(sumReadRate * 100) / 100.0;
            StatsLog.logger.info("Total Read Rate=" + sumReadRate + " per ms");

            StatsLog.logger.info("writer latency stats:");
            writers[0].getLatencyStats().print(StatsLog.logger);

            if (!doValidation) {
                StatsLog.logger.info("reader latency stats:");
                readers[0].getLatencyStats().print(StatsLog.logger);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    @Override
    public void run(int numOfReaders, int numOfWriters, int runDuration) {
        try {
            int timeAllocated = Math.round((float)runDuration/3);
            
            StatsLog.logger.info(">>> populate");
            populate();
            
            StatsLog.logger.info(">>> read only");
            evalRead(numOfReaders, Math.min(timeAllocated, 10));
            
            StatsLog.logger.info(">>> write only");
            evalWrite(numOfWriters, timeAllocated);
            
            StatsLog.logger.info(">>> validate");
            validate();
            
            StatsLog.logger.info(">>> read & write");
            evalReadWrite(numOfReaders, numOfWriters, timeAllocated, false);
            
            StatsLog.logger.info(">>> validate");
            validate();

            StatsLog.logger.info(">>> check & write");
            evalReadWrite(numOfReaders, numOfWriters, timeAllocated, true);
            
            StatsLog.logger.info(">>> validate");
            validate();
        } catch (Exception e) {
            StatsLog.logger.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }
}
