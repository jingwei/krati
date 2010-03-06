package test.perf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import krati.mds.MDSCache;
import krati.mds.MDSLoader;
import krati.mds.impl.MDSCacheImpl;
import krati.mds.impl.MDSLoaderImpl;
import krati.zoie.impl.ZoieMDSCache;
import krati.zoie.impl.ZoieMDSInterpreter;

import org.apache.lucene.index.IndexReader;

import proj.zoie.impl.indexing.DefaultIndexReaderDecorator;
import proj.zoie.impl.indexing.ZoieSystem;

public class EvalMDSCache
{
    static List<String> _lineSeedData = new ArrayList<String>(3000);
    
    public void loadSeedData(File dataFile) throws IOException
    {
        String line;
        FileReader reader = new FileReader(dataFile);
        BufferedReader in = new BufferedReader(reader);
        
        while((line = in.readLine()) != null)
        {
            _lineSeedData.add(line);
        }
        
        in.close();
        reader.close();
    }
    
    public void populate(MDSCache mds) throws IOException
    {
        String line;
        int lineCnt = _lineSeedData.size();
        int index = mds.getIdStart();
        int stopIndex = index + mds.getIdCount();
        
        long scn = mds.getHWMark();
        long startTime = System.currentTimeMillis();
        
        while(index < stopIndex)
        {
            try
            {   line = _lineSeedData.get(index % lineCnt);
                mds.setData(index, line.getBytes(), scn++);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            index++;
        }
        
        mds.persist();

        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.printf("elapsedTime=%d ms (init)%n", elapsedTime);
        
        double rate = mds.getIdCount()/(double)elapsedTime;
        System.out.printf("writeCount=%d rate=%6.2f per ms%n", mds.getIdCount(), rate);
    }
    
    public static void checkData(MDSCache mds, int index)
    {
        byte[] b = mds.getData(index);
        if (b != null)
        {
            String s = new String(b);
            System.out.printf("[%8d] %s%n", index, s);
        }
        else
        {
            System.out.printf("[%8d] %s%n", index, null);
        }
    }
    
    public static void checkData(MDSCache mds)
    {
        checkData(mds, 0);
        checkData(mds, 10);
        checkData(mds, 100);
        checkData(mds, 1000);
        checkData(mds, 10000);
        checkData(mds, 100000);
        checkData(mds, 1000000);
        checkData(mds, 2000000);
        checkData(mds, 3000000);
        checkData(mds, 4000000);
    }
    
    static class Reader implements Runnable
    {
        MDSCache _mds;
        Random _rand = new Random();
        byte[] _data = new byte[1 << 13];
        boolean _running = true;
        int _indexStart;
        int _length;
        long _cnt = 0;
        
        public Reader(MDSCache mds)
        {
            this._mds = mds;
            this._length = mds.getIdCount();
            this._indexStart = mds.getIdStart();
        }
        
        public long getReadCount()
        {
            return this._cnt;
        }

        public void stop()
        {
            _running = false;
        }
        
        int read(int index)
        {
            return _mds.getData(index, _data);
        }
        
        @Override
        public void run()
        {
            while(_running)
            {
                read(_indexStart + _rand.nextInt(_length));
                _cnt++;
            }
        }
    }
    
    static class Writer implements Runnable
    {
        MDSCache _mds;
        Random _rand = new Random();
        boolean _running = true;
        
        int _indexStart;
        int _length;
        long _cnt = 0;
        long _scn = 0;
        
        public Writer(MDSCache mds)
        {
            this._mds = mds;
            this._length = mds.getIdCount();
            this._indexStart = mds.getIdStart();
            this._scn = mds.getHWMark();
        }
        
        public long getWriteCount()
        {
            return this._cnt;
        }

        public void stop()
        {
            _running = false;
        }
        
        void write(int index)
        {
            try
            {
                byte[] b = _lineSeedData.get(index%_lineSeedData.size()).getBytes();
                _mds.setData(index, b, _scn++);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
        
        @Override
        public void run()
        {
            while(_running)
            {
                write(_indexStart + _rand.nextInt(_length));
                _cnt++;
            }
        }
    }
    
    public static double testWrite(MDSCache mds, int runTimeSeconds) throws Exception
    {
        try
        {
            // Start writer
            Writer writer = new Writer(mds);
            Thread writerThread = new Thread(writer);
            writerThread.start();
            System.out.println("Writer started");
            
            long startTime = System.currentTimeMillis();
            long writeCount = 0;
            int heartBeats = runTimeSeconds/10;
            for(int i = 0; i < heartBeats; i++)
            {
                Thread.sleep(10000);
                long newWriteCount = writer.getWriteCount();
                System.out.printf("writeCount=%d%n", newWriteCount - writeCount);
                writeCount = newWriteCount;
            }
            
            writer.stop();
            writerThread.join();
            
            long endTime = System.currentTimeMillis();

            long elapsedTime = endTime - startTime;
            System.out.printf("elapsedTime=%d ms%n", elapsedTime);
            
            double rate = writer.getWriteCount()/(double)elapsedTime;
            System.out.printf("writeCount=%d rate=%6.2f per ms%n", writer.getWriteCount(), rate);
            
            return rate;
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw e;
        }
    }
    
    public static void testRead(MDSCache mds, int readerCnt, int runTimeSeconds) throws Exception
    {
        try
        {
            // Start readers
            Reader[] readers = new Reader[readerCnt];
            for(int i = 0; i < readers.length; i++)
            {
                readers[i] = new Reader(mds);
            }
            
            Thread[] threads = new Thread[readers.length];
            for(int i = 0; i < threads.length; i++)
            {
                threads[i] = new Thread(readers[i]);
                threads[i].start();
                System.out.println("Reader " + i + " started");
            }
            
            long startTime = System.currentTimeMillis();
            
            // Sleep until run time is exhausted
            Thread.sleep(runTimeSeconds * 1000);
            
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
            System.out.printf("elapsedTime=%d ms%n", elapsedTime);
            for(int i = 0; i < readers.length; i++)
            {
                double rate = readers[i].getReadCount()/(double)elapsedTime;
                System.out.printf("readCount[%d]=%d rate=%6.2f per ms%n", i, readers[i].getReadCount(), rate);
                sumReadRate += rate;
            }
            
            System.out.printf("Total Read Rate=%6.2f per ms%n", sumReadRate);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw e;
        }
    }
    
    public static void testReadWrite(MDSCache mds, int readerCnt, int runTimeSeconds) throws Exception
    {
        try
        {
            // Start readers
            Reader[] readers = new Reader[readerCnt];
            for(int i = 0; i < readers.length; i++)
            {
                readers[i] = new Reader(mds);
            }

            Thread[] threads = new Thread[readers.length];
            for(int i = 0; i < threads.length; i++)
            {
                threads[i] = new Thread(readers[i]);
                threads[i].start();
                System.out.println("Reader " + i + " started");
            }
            
            // Start writer
            Writer writer = new Writer(mds);
            Thread writerThread = new Thread(writer);
            writerThread.start();
            System.out.println("Writer started");
            
            long startTime = System.currentTimeMillis();
            
            long readCount = 0;
            long writeCount = 0;
            int heartBeats = runTimeSeconds/10;
            for(int i = 0; i < heartBeats; i++)
            {
                Thread.sleep(10000);

                int newReadCount = 0;
                for(int r = 0; r < readers.length; r++)
                {
                    newReadCount += readers[r].getReadCount();
                }
                
                long newWriteCount = writer.getWriteCount();
                
                System.out.printf("write=%d read=%d%n", newWriteCount-writeCount, newReadCount-readCount);
                
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
            System.out.printf("elapsedTime=%d ms%n", elapsedTime);
            
            double rate = writer.getWriteCount()/(double)elapsedTime;
            System.out.printf("writeCount=%d rate=%6.2f per ms%n", writer.getWriteCount(), rate);
            
            double sumReadRate = 0;
            for(int i = 0; i < readers.length; i++)
            {
                rate = readers[i].getReadCount()/(double)elapsedTime;
                System.out.printf("readCount[%d]=%d rate=%6.2f per ms%n", i, readers[i].getReadCount(), rate);
                sumReadRate += rate;
            }
            
            System.out.printf("Total Read Rate=%6.2f per ms%n", sumReadRate);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw e;
        }
    }
    
    static MDSCache getMDSCache(File mdsCacheDir) throws Exception
    {
    	int idStart = 0;
        int idCount = 500000;
        int segFileSizeMB = 256;
        
        MDSCache mds = new MDSCacheImpl(idStart,
                                        idCount,
                                        mdsCacheDir,
                                        new krati.mds.impl.segment.MemorySegmentFactory(),
                                        segFileSizeMB);
        return mds;
    }
    
    @SuppressWarnings("unchecked")
    static MDSCache getMDSCacheZoie(File mdsCacheDir) throws Exception
    {
        int idStart = 0;
        int idCount = 500000;
        
    	ZoieSystem zs = new ZoieSystem(mdsCacheDir,
    	                               new ZoieMDSInterpreter(),
    	                               new DefaultIndexReaderDecorator(),
    	                               null, null, 10000, 60000, true);
    	zs.start();
    	
    	MDSCache mds = new ZoieMDSCache<IndexReader>(zs, idStart, idCount);
        return mds;
    }
    
    public static void main(String[] args)
    {
        EvalMDSCache mdsTest = new EvalMDSCache();
        
        try
        {
            File seedDataFile = new File("test/mds_seed/seed.dat");
            mdsTest.loadSeedData(seedDataFile);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            return;
        }
        
        try
        {
            MDSCache mds;
            MDSLoader mdsLoader = new MDSLoaderImpl();
            
            File mdsCacheDir = new File("test/mds_cache");
            
            mds = getMDSCache(mdsCacheDir);
            // mds = getMDSCacheZoie(mdsCacheDir);
            
            int runTimeSeconds = 60;
            File mdsInitFile = new File("test/mds.init.dat");
            File mdsDumpFile = new File("test/mds.dump.dat");
            
            System.out.println("---checkData---");
            checkData(mds);
            
            if (mds.getLWMark() == 0)
            {
                System.out.println("---populate---");
                mdsTest.populate(mds);
                
                System.out.println("---dump---");
                mdsLoader.dump(mds, mdsInitFile);
                
                System.out.println("---checkData---");
                checkData(mds);
            }
            
            System.out.println("---testWrite---");
            testWrite(mds, runTimeSeconds);
            mds.persist();
            
            System.out.println("---checkData---");
            checkData(mds);
            
            System.out.println("---testRead---");
            testRead(mds, 4, runTimeSeconds);
            
            System.out.println("---checkData---");
            checkData(mds);
            
            System.out.println("---testReadWrite---");
            testReadWrite(mds, 4, runTimeSeconds);
            mds.persist();
            
            System.out.println("---checkData---");
            checkData(mds);
            
            System.out.println("---dump---");
            mdsLoader.dump(mds, mdsDumpFile);
            
            System.out.println("done");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}
