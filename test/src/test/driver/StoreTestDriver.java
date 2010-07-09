package test.driver;

import java.util.List;

import test.StatsLog;

/**
 * Data Store Test Driver.
 *  
 * @author jwu
 *
 * @param <S> Data Store
 */
public class StoreTestDriver<S>
{
    private final S _store;
    private final StoreReader<S, String, String> _storeReader;
    private final StoreWriter<S, String, String> _storeWriter;
    private final List<String> _lineSeedData;

    public StoreTestDriver(S store,
                           StoreReader<S, String, String> storeReader,
                           StoreWriter<S, String, String> storeWriter,
                           List<String> lineSeedData)
    {
        this._store = store;
        this._storeReader = storeReader;
        this._storeWriter = storeWriter;
        this._lineSeedData = lineSeedData;
    }

    public void validate() throws Exception
    {
        int count = 0;
        
        for(int i = 0; i < 100; i++)
        {
            for(String line : _lineSeedData)
            {
                if(line.length() > (30 + i * 3))
                {
                    count++;
                    String key = line.substring(0, 30 + i * 3);
                    String value = _storeReader.get(_store, key);
                    if(value != null) {
                        if(!line.equals(value)) {
                            System.err.printf("key=\"%s\"%n", key);
                            System.err.printf("    \"%s\"%n", line);
                            System.err.printf("    \"%s\"%n", value);
                        }
                    }
                }
            }
        }
        
        StatsLog.logger.info("OK");
    }
    
    public void populate()
    {
        int count = 0;
        
        long startTime = System.currentTimeMillis();
        
        for(int i = 0; i < 100; i++)
        {
            for(String line : _lineSeedData)
            {
                if(line.length() > (30 + i * 3))
                {
                    count++;
                    String key = line.substring(0, 30 + i * 3);
                    _storeWriter.put(_store, key, line);
                }
            }
        }
        
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        StatsLog.logger.info("elapsedTime=" + elapsedTime + " ms (init)");
        
        double rate;
        rate = count/(double)elapsedTime;
        rate = Math.round(rate * 100)/100.0;
        StatsLog.logger.info("writeCount=" + count + " rate=" + rate + " per ms");
    }
    
    @SuppressWarnings("unchecked")
    public void evalWrite(int writerCnt, int runDuration) throws Exception
    {
        try
        {
            // Start writers
            WriteDriver<S>[] writers = new WriteDriver[writerCnt];
            for(int i = 0; i < writers.length; i++)
            {
                writers[i] = new WriteDriver<S>(_store, _storeWriter, _lineSeedData);
            }
            
            Thread[] writerThreads = new Thread[writers.length];
            for(int i = 0; i < writerThreads.length; i++)
            {
                writerThreads[i] = new Thread(writers[i]);
                writerThreads[i].start();
                StatsLog.logger.info("Writer " + i + " started");
            }
            
            long startTime = System.currentTimeMillis();
            long writeCount = 0;
            int heartBeats = runDuration/10;
            for(int i = 0; i < heartBeats; i++)
            {
                Thread.sleep(10000);
                long newWriteCount = 0;
                for(int r = 0; r < writers.length; r++)
                {
                    newWriteCount += writers[r].getWriteCount();
                }
                
                StatsLog.logger.info("writeCount=" + (newWriteCount - writeCount));
                writeCount = newWriteCount;
            }
            
            // Stop writer
            for(int i = 0; i < writers.length; i++)
            {
                writers[i].stop();
            }
            for(int i = 0; i < writerThreads.length; i++)
            {
                writerThreads[i].join();
            }
            
            long endTime = System.currentTimeMillis();

            long elapsedTime = endTime - startTime;
            StatsLog.logger.info("elapsedTime=" + elapsedTime + " ms");
            
            double sumWriteRate = 0;
            for(int i = 0; i < writers.length; i++)
            {
                double rate = writers[i].getWriteCount()/(double)elapsedTime;
                rate = Math.round(rate * 100) / 100.0;
                StatsLog.logger.info("writeCount["+ i +"]="+ writers[i].getWriteCount() + " rate=" + rate + " per ms");
                sumWriteRate += rate;
            }

            StatsLog.logger.info("Total Write Rate=" + sumWriteRate + " per ms");
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw e;
        }
    }
    
    @SuppressWarnings("unchecked")
    public void evalRead(int readerCnt, int runDuration) throws Exception
    {
        try
        {
            // Start readers
            ReadDriver<S>[] readers = new ReadDriver[readerCnt];
            for(int i = 0; i < readers.length; i++)
            {
                readers[i] = new ReadDriver<S>(_store, _storeReader, _lineSeedData);
            }
            
            Thread[] threads = new Thread[readers.length];
            for(int i = 0; i < threads.length; i++)
            {
                threads[i] = new Thread(readers[i]);
                threads[i].start();
                StatsLog.logger.info("Reader " + i + " started");
            }
            
            long startTime = System.currentTimeMillis();
            
            // Sleep until run time is exhausted
            Thread.sleep(runDuration * 1000);
            
            for(int i = 0; i < readers.length; i++)
            {
                readers[i].stop();
            }
            for(int i = 0; i < threads.length; i++)
            {
                threads[i].join();
            }
            
            long endTime = System.currentTimeMillis();
            
            double sumReadRate = 0;
            long elapsedTime = endTime - startTime;
            StatsLog.logger.info("elapsedTime=" + elapsedTime + " ms");
            for(int i = 0; i < readers.length; i++)
            {
                double rate = readers[i].getReadCount()/(double)elapsedTime;
                rate = Math.round(rate * 100) / 100.0;
                StatsLog.logger.info("readCount[" + i + "]=" + readers[i].getReadCount() + " rate=" + rate + " per ms");
                sumReadRate += rate;
            }
            
            StatsLog.logger.info("Total Read Rate=" + sumReadRate + " per ms");
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw e;
        }
    }
    
    @SuppressWarnings("unchecked")
    public void evalReadWrite(int readerCnt, int writerCnt, int runDuration, boolean doValidation) throws Exception
    {
        try
        {
            // Start readers
            ReadDriver<S>[] readers = new ReadDriver[readerCnt];
            for(int i = 0; i < readers.length; i++)
            {
                readers[i] = doValidation ?
                        new CheckDriver<S>(_store, _storeReader, _lineSeedData) :
                        new ReadDriver<S>(_store, _storeReader, _lineSeedData);
            }

            Thread[] readerThreads = new Thread[readers.length];
            for(int i = 0; i < readerThreads.length; i++)
            {
                readerThreads[i] = new Thread(readers[i]);
                readerThreads[i].start();
                StatsLog.logger.info("Reader " + i + " started");
            }
            
            // Start writers
            WriteDriver<S>[] writers = new WriteDriver[writerCnt];
            for(int i = 0; i < writers.length; i++)
            {
                writers[i] = new WriteDriver<S>(_store, _storeWriter, _lineSeedData);
            }
            
            Thread[] writerThreads = new Thread[writers.length];
            for(int i = 0; i < writerThreads.length; i++)
            {
                writerThreads[i] = new Thread(writers[i]);
                writerThreads[i].start();
                StatsLog.logger.info("Writer " + i + " started");
            }
            
            long startTime = System.currentTimeMillis();
            
            long readCount = 0;
            long writeCount = 0;
            int heartBeats = runDuration/10;
            for(int i = 0; i < heartBeats; i++)
            {
                Thread.sleep(10000);

                long newReadCount = 0;
                for(int r = 0; r < readers.length; r++)
                {
                    newReadCount += readers[r].getReadCount();
                }
                
                long newWriteCount = 0;
                for(int r = 0; r < writers.length; r++)
                {
                    newWriteCount += writers[r].getWriteCount();
                }
                
                StatsLog.logger.info("write=" + (newWriteCount-writeCount) + " read=" + (newReadCount-readCount));
                
                readCount = newReadCount;
                writeCount = newWriteCount;
            }
            
            // Stop reader
            for(int i = 0; i < readers.length; i++)
            {
                readers[i].stop();
            }
            for(int i = 0; i < readerThreads.length; i++)
            {
                readerThreads[i].join();
            }
            
            // Stop writer
            for(int i = 0; i < writers.length; i++)
            {
                writers[i].stop();
            }
            for(int i = 0; i < writerThreads.length; i++)
            {
                writerThreads[i].join();
            }
            
            long endTime = System.currentTimeMillis();

            long elapsedTime = endTime - startTime;
            StatsLog.logger.info("elapsedTime=" + elapsedTime + " ms");
            
            double sumWriteRate = 0;
            for(int i = 0; i < writers.length; i++)
            {
                double rate = writers[i].getWriteCount()/(double)elapsedTime;
                rate = Math.round(rate * 100) / 100.0;
                StatsLog.logger.info("writeCount[" + i + "]=" + writers[i].getWriteCount() + " rate=" + rate + " per ms");
                sumWriteRate += rate;
            }

            StatsLog.logger.info("Total Write Rate=" + sumWriteRate + " per ms");
            
            double sumReadRate = 0;
            for(int i = 0; i < readers.length; i++)
            {
                double rate = readers[i].getReadCount()/(double)elapsedTime;
                StatsLog.logger.info("readCount[" + i + "]=" + readers[i].getReadCount() + " rate=" + rate);
                sumReadRate += rate;
            }
            
            StatsLog.logger.info("Total Read Rate=" + sumReadRate + " per ms");
            
            StatsLog.logger.info("writer latency stats:");
            writers[0].getLatencyStats().print(StatsLog.logger);
            
            if(!doValidation)
            {
                StatsLog.logger.info("reader latency stats:");
                readers[0].getLatencyStats().print(StatsLog.logger);
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw e;
        }
    }
    
    public void run(int numOfReaders, int numOfWriters, int runDuration)
    {
        try
        {
            int timeAllocated = runDuration/3;
            
            StatsLog.logger.info(">>> populate");
            populate();
            
            StatsLog.logger.info(">>> read only");
            evalRead(numOfReaders, 10);
            
            StatsLog.logger.info(">>> write only");
            evalWrite(numOfWriters, timeAllocated);
            
            StatsLog.logger.info(">>> validate");
            validate();
            
            StatsLog.logger.info(">>> read & write");
            evalReadWrite(numOfReaders, numOfWriters, timeAllocated, false);
            
            StatsLog.logger.info(">>> validate");
            validate();

            StatsLog.logger.info(">>> check& write");
            evalReadWrite(numOfReaders, numOfWriters, timeAllocated, true);
            
            StatsLog.logger.info(">>> validate");
            validate();
        }
        catch(Exception e)
        {
            StatsLog.logger.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }
}
