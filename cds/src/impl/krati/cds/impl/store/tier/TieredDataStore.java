package krati.cds.impl.store.tier;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import krati.cds.impl.segment.MappedSegmentFactory;
import krati.cds.impl.segment.SegmentFactory;
import krati.cds.store.DataStore;
import krati.util.FnvHashFunction;
import krati.util.HashFunction;

/**
 * TieredDataStore
 * 
 * @author jwu
 *
 */
public class TieredDataStore implements DataStore<byte[], byte[]>
{
    final static Logger _log = Logger.getLogger(TieredDataStore.class);
    
    final NonCollidingTier _tier1;
    final NonCollidingTier _tier2;
    final CollidingTier    _tier3;
    final SplitRunner      _splitRunner = new SplitRunner();
    
    /**
     * Creates a TieredDataStore.
     * 
     * <pre>
     *    Entry Size               : 10000
     *    Max Entries              : 5
     *    Segment Factory          : krati.cds.impl.segment.MappedSegmentFactory
     *    Segment Compact Trigger  : 0.1
     *    Segment Compact Factor   : 0.5
     *    Store Hash Function      : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir                the home directory of DataStore
     * @param initLevel              the initial level when DataStore is created
     * @param segmentFileSizeMB      the size of segment file in MB
     * @throws Exception             if this tiered data store cannot be created.
     */
    public TieredDataStore(File homeDir,
                           int initLevel,
                           int segmentFileSizeMB) throws Exception
    {
        this(homeDir,
             initLevel,
             10000, /* entrySize */
             5,     /* maxEntries */
             segmentFileSizeMB,
             new MappedSegmentFactory(),
             0.1,   /* segmentCompactTrigger */
             0.5,   /* segmentCompactFactor  */
             new FnvHashFunction());
    }
    
    /**
     * Creates a TieredDataStore.
     * 
     * <pre>
     *    Entry Size               : 10000
     *    Max Entries              : 5
     *    Segment Compact Trigger  : 0.1
     *    Segment Compact Factor   : 0.5
     *    Store Hash Function      : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir                the home directory of DataStore
     * @param initLevel              the initial level when DataStore is created
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @throws Exception             if this tiered data store cannot be created.
     */
    public TieredDataStore(File homeDir,
                           int initLevel,
                           int segmentFileSizeMB,
                           SegmentFactory segmentFactory) throws Exception
    {
        this(homeDir,
             initLevel,
             10000, /* entrySize */
             5,     /* maxEntries */
             segmentFileSizeMB,
             segmentFactory,
             0.1,   /* segmentCompactTrigger */
             0.5,   /* segmentCompactFactor  */
             new FnvHashFunction());
    }
    
    /**
     * Creates a TieredDataStore.
     * 
     * <pre>
     *    Entry Size               : 10000
     *    Max Entries              : 5
     *    Segment Compact Trigger  : 0.1
     *    Segment Compact Factor   : 0.5
     * </pre>
     * 
     * @param homeDir                the home directory of DataStore
     * @param initLevel              the initial level when DataStore is created
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @param hashFunction           the hash function for mapping keys to indexes
     * @throws Exception             if this tiered data store cannot be created.
     */
    public TieredDataStore(File homeDir,
                           int initLevel,
                           int segmentFileSizeMB,
                           SegmentFactory segmentFactory,
                           HashFunction<byte[]> hashFunction) throws Exception
    {
        this(homeDir,
             initLevel,
             10000, /* entrySize */
             5,     /* maxEntries */
             segmentFileSizeMB,
             segmentFactory,
             0.1,   /* segmentCompactTrigger */
             0.5,   /* segmentCompactFactor  */
             hashFunction);
    }
    
    /**
     * Creates a TieredDataStore.
     * 
     * <pre>
     *    Segment Compact Trigger  : 0.1
     *    Segment Compact Factor   : 0.5
     * </pre>
     * 
     * @param homeDir                the home directory of DataStore
     * @param initLevel              the initial level when DataStore is created
     * @param entrySize              the redo entry size (i.e., batch size)
     * @param maxEntries             the number of redo entries required for updating the underlying address array
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @param hashFunction           the hash function for mapping keys to indexes
     * @throws Exception             if this tiered data store cannot be created.
     */
    public TieredDataStore(File homeDir,
                           int initLevel,
                           int entrySize,
                           int maxEntries,
                           int segmentFileSizeMB,
                           SegmentFactory segmentFactory,
                           HashFunction<byte[]> hashFunction) throws Exception
    {
        this(homeDir,
             initLevel,
             entrySize,
             maxEntries,
             segmentFileSizeMB,
             segmentFactory,
             0.1 /* segmentCompactTrigger */,
             0.5 /* segmentCompactFactor  */,
             hashFunction);
    }
    
    /**
     * Creates a TieredDataStore.
     * 
     * @param homeDir                the home directory of DataStore
     * @param initLevel              the initial level when DataStore is created
     * @param entrySize              the redo entry size (i.e., batch size)
     * @param maxEntries             the number of redo entries required for updating the underlying address array
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @param segmentCompactTrigger  the percentage of segment capacity, which triggers compaction once per segment
     * @param segmentCompactFactor   the load factor of segment, below which a segment is eligible for compaction
     * @param hashFunction           the hash function for mapping keys to indexes
     * @throws Exception             if this tiered data store cannot be created.
     */
    public TieredDataStore(File homeDir,
                           int initLevel,
                           int entrySize,
                           int maxEntries,
                           int segmentFileSizeMB,
                           SegmentFactory segmentFactory,
                           double segmentCompactTrigger,
                           double segmentCompactFactor,
                           HashFunction<byte[]> hashFunction) throws Exception
    {
        _tier1 = initNonCollidingTier(
                new File(homeDir, "T1"),
                initLevel + 2,
                entrySize,
                maxEntries,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactTrigger,
                segmentCompactFactor,
                hashFunction);
        
        _tier2 = initNonCollidingTier(
                new File(homeDir, "T2"),
                initLevel + 1,
                entrySize,
                maxEntries,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactTrigger,
                segmentCompactFactor,
                hashFunction);
        
        _tier3 = initCollisionTier(
                new File(homeDir, "T3"),
                initLevel,
                entrySize,
                maxEntries,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactTrigger,
                segmentCompactFactor,
                0.5, /* hash load factor */
                hashFunction);
    }
    
    protected CollidingTier initCollisionTier(
            File homeDir,
            int initLevel,
            int entrySize,
            int maxEntries,
            int segmentFileSizeMB,
            SegmentFactory segmentFactory,
            double segmentCompactTrigger,
            double segmentCompactFactor,
            double hashLoadFactor,
            HashFunction<byte[]> hashFunction) throws Exception
    {
        return new CollidingTier(
                this,
                homeDir,
                initLevel,
                entrySize,
                maxEntries,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactTrigger,
                segmentCompactFactor,
                hashLoadFactor,
                hashFunction);
    }
    
    protected NonCollidingTier initNonCollidingTier(
            File homeDir,
            int initLevel,
            int entrySize,
            int maxEntries,
            int segmentFileSizeMB,
            SegmentFactory segmentFactory,
            double segmentCompactTrigger,
            double segmentCompactFactor,
            HashFunction<byte[]> hashFunction) throws Exception
    {
        return new NonCollidingTier(
                homeDir,
                initLevel,
                entrySize,
                maxEntries,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactTrigger,
                segmentCompactFactor,
                hashFunction);
    }
    
    @Override
    public synchronized void clear() throws IOException
    {
        _tier1.clear();
        _tier2.clear();
        _tier3.clear();
    }
    
    @Override
    public synchronized void persist() throws IOException
    {
        _tier1.persist();
        _tier2.persist();
        _tier3.persist();
    }
    
    @Override
    public synchronized void sync() throws IOException
    {
        _tier1.sync();
        _tier2.sync();
        _tier3.sync();
    }
    
    @Override
    public byte[] get(byte[] key)
    {
        if (key == null) return null;
        
        byte[] result = _tier1.get(key);
        if(result != null) return result;
        
        result = _tier2.get(key);
        if(result != null) return result;
        
        return _tier3.get(key);
    }
    
    @Override
    public synchronized boolean put(byte[] key, byte[] value) throws Exception
    {
        if(key == null) return false;

        trySplitCollidingTier();
        
        if(!_tier1.put(key, value))
        {
            if(!_tier2.put(key, value))
            {
                return _tier3.put(key, value);
            }
        }
        
        return true;
    }
    
    @Override
    public synchronized boolean delete(byte[] key) throws Exception
    {
        if(key == null) return false;

        trySplitCollidingTier();
        
        if(!_tier1.delete(key))
        {
            if(!_tier2.delete(key))
            {
                return _tier3.delete(key);
            }
        }
        
        return true;
    }
    
    synchronized void startSplitNonCollidingTiers()
    {
        _tier1.forceSplit();
        _tier2.forceSplit();
        
        new Thread(_splitRunner).start();
    }
    
    synchronized void splitDone()
    {
        //  Make sure that tier1 and tier2 finish
        _tier1.rehash();
        _tier2.rehash();
        
        _log.info("Tier1 Split Done " + _tier1.getStatus());
        _log.info("Tier2 Split Done " + _tier2.getStatus());
        _log.info("Tier3 Split Done " + _tier3.getStatus());
    }
    
    private void trySplitCollidingTier()
    {
        _tier3.trySplit();
    }
    
    class SplitRunner implements Runnable
    {
        @Override
        public void run()
        {
            _log.info("Tier1 Split Run: " + _tier1.getStatus());
            _tier1.rehash();
            _log.info("Tier1 Split End: " + _tier1.getStatus());
            
            _log.info("Tier2 Split Run: " + _tier2.getStatus());
            _tier2.rehash();
            _log.info("Tier2 Split End: " + _tier2.getStatus());
        }
    }
}
