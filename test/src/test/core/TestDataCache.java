package test.core;

import java.io.File;
import java.io.IOException;

import krati.core.segment.SegmentFactory;
import krati.store.DataCache;
import krati.store.DataCacheImpl;

import test.AbstractSeedTest;
import test.StatsLog;
import test.util.DataCacheChecker;
import test.util.DataCacheReader;
import test.util.DataCacheWriter;

/**
 * TestDataCache using MemorySegment 
 * 
 * @author jwu
 *
 */
public class TestDataCache extends AbstractSeedTest
{
    public TestDataCache()
    {
        super(TestDataCache.class.getName());
    }
    
    public void populate(DataCache cache) throws IOException
    {
        String line;
        int lineCnt = _lineSeedData.size();
        int index = cache.getIdStart();
        int stopIndex = index + cache.getIdCount();
        
        long scn = cache.getHWMark();
        long startTime = System.currentTimeMillis();
        
        while(index < stopIndex)
        {
            try
            {
                line = _lineSeedData.get(index % lineCnt);
                cache.set(index, line.getBytes(), scn++);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            index++;
        }
        
        cache.persist();

        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
        
        double rate = cache.getIdCount()/(double)elapsedTime;
        rate = Math.round(rate * 100) / 100.0;
        StatsLog.logger.info("writeCount="+ cache.getIdCount() +" rate="+ rate +" per ms");
    }
    
    public static void checkData(DataCache cache, int index)
    {
        String line = _lineSeedData.get(index % _lineSeedData.size());
        
        byte[] b = cache.get(index);
        if (b != null)
        {
            String s = new String(b);
            assertTrue("[" + index + "]=" + s + " expected=" + line, s.equals(line));
        }
        else
        {
            assertTrue("[" + index + "]=null", line == null);
        }
    }
    
    public static void validate(DataCache cache)
    {
        int cacheSize = cache.getIdCount();
        for(int i = 0; i < cacheSize; i++)
        {
            int index = cache.getIdStart() + i;
            checkData(cache, index);
        }
        StatsLog.logger.info("OK");
    }
    
    public static double evalWrite(DataCache cache, int runDuration) throws Exception
    {
        try
        {
            // Start writer
            DataCacheWriter writer = new DataCacheWriter(cache, _lineSeedData);
            Thread writerThread = new Thread(writer);
            writerThread.start();
            StatsLog.logger.info("Writer started");
            
            long startTime = System.currentTimeMillis();
            long writeCount = 0;
            int heartBeats = runDuration/10;
            long sleepTime = Math.min(runDuration * 1000, 10000);
            for(int i = 0; i < heartBeats; i++)
            {
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
        catch(Exception e)
        {
            e.printStackTrace();
            throw e;
        }
    }
    
    public static void evalRead(DataCache cache, int readerCnt, int runDuration) throws Exception
    {
        try
        {
            // Start readers
            DataCacheReader[] readers = new DataCacheReader[readerCnt];
            for(int i = 0; i < readers.length; i++)
            {
                readers[i] = new DataCacheReader(cache, _lineSeedData);
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
            StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
            for(int i = 0; i < readers.length; i++)
            {
                double rate = readers[i].getReadCount()/(double)elapsedTime;
                rate = Math.round(rate * 100) / 100.0;
                StatsLog.logger.info("readCount["+ i +"]="+ readers[i].getReadCount() +" rate="+ rate +" per ms");
                sumReadRate += rate;
            }
            
            sumReadRate = Math.round(sumReadRate * 100) / 100.0;
            StatsLog.logger.info("Total Read Rate="+ sumReadRate +" per ms");
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw e;
        }
    }
    
    
    public static void evalReadWrite(DataCache cache, int readerCnt, int runDuration, boolean doValidation) throws Exception
    {
        try
        {
            // Start readers
            DataCacheReader[] readers = new DataCacheReader[readerCnt];
            for(int i = 0; i < readers.length; i++)
            {
                readers[i] = doValidation ? new DataCacheChecker(cache, _lineSeedData) : new DataCacheReader(cache, _lineSeedData);
            }

            Thread[] threads = new Thread[readers.length];
            for(int i = 0; i < threads.length; i++)
            {
                threads[i] = new Thread(readers[i]);
                threads[i].start();
                StatsLog.logger.info("Reader " + i + " started");
            }
            
            // Start writer
            DataCacheWriter writer = new DataCacheWriter(cache, _lineSeedData);
            Thread writerThread = new Thread(writer);
            writerThread.start();
            StatsLog.logger.info("Writer started");
            
            long startTime = System.currentTimeMillis();
            
            long readCount = 0;
            long writeCount = 0;
            int heartBeats = runDuration/10;
            long sleepTime = Math.min(runDuration * 1000, 10000);
            for(int i = 0; i < heartBeats; i++)
            {
                Thread.sleep(sleepTime);

                long newReadCount = 0;
                for(int r = 0; r < readers.length; r++)
                {
                    newReadCount += readers[r].getReadCount();
                }
                
                long newWriteCount = writer.getWriteCount();
                
                StatsLog.logger.info("write="+ (newWriteCount-writeCount) +" read=" + (newReadCount-readCount));
                
                readCount = newReadCount;
                writeCount = newWriteCount;
            }
            
            // Stop reader
            for(int i = 0; i < readers.length; i++)
            {
                readers[i].stop();
            }
            for(int i = 0; i < threads.length; i++)
            {
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
            for(int i = 0; i < readers.length; i++)
            {
                rate = readers[i].getReadCount()/(double)elapsedTime;
                rate = Math.round(rate * 100) / 100.0;
                StatsLog.logger.info("readCount["+ i +"]="+ readers[i].getReadCount() +" rate="+ rate +" per ms");
                sumReadRate += rate;
            }
            
            sumReadRate = Math.round(sumReadRate * 100) / 100.0;
            StatsLog.logger.info("Total Read Rate="+ sumReadRate +" per ms");
            
            StatsLog.logger.info("writer latency stats:");
            writer.getLatencyStats().print(StatsLog.logger);
            
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
    
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.core.segment.MemorySegmentFactory();
    }
    
    protected DataCache getDataCache(File cacheDir) throws Exception
    {
        DataCache cache = new DataCacheImpl(_idStart,
                                            _idCount,
                                            cacheDir,
                                            getSegmentFactory(),
                                            _segFileSizeMB);
        return cache;
    }
    
    public void testDataCache() throws Exception
    {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        TestDataCache eval = new TestDataCache();
        
        try
        {
            AbstractSeedTest.loadSeedData();
        }
        catch(Exception e)
        {
            e.printStackTrace();
            return;
        }
        
        try
        {
            File cacheDir = getHomeDirectory();
            DataCache cache = getDataCache(cacheDir);
            
            if (cache.getLWMark() == 0)
            {
                StatsLog.logger.info(">>> populate");
                eval.populate(cache);
                
                StatsLog.logger.info(">>> validate");
                validate(cache);
            }
            
            int timeAllocated = Math.round((float)_runTimeSeconds/3);

            StatsLog.logger.info(">>> read only");
            evalRead(cache, _numReaders, Math.min(timeAllocated, 10));
            
            StatsLog.logger.info(">>> write only");
            evalWrite(cache, timeAllocated);
            cache.persist();
            
            StatsLog.logger.info(">>> validate");
            validate(cache);
            
            StatsLog.logger.info(">>> read & write");
            evalReadWrite(cache, _numReaders, timeAllocated, false);
            cache.persist();
            
            StatsLog.logger.info(">>> validate");
            validate(cache);
            
            StatsLog.logger.info(">>> check & write");
            evalReadWrite(cache, _numReaders, timeAllocated, true);
            cache.persist();
            
            StatsLog.logger.info(">>> validate");
            validate(cache);
            
            cache.sync();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
