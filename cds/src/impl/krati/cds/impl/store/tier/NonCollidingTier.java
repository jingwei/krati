package krati.cds.impl.store.tier;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import krati.cds.impl.segment.SegmentFactory;
import krati.cds.store.DataStore;
import krati.util.HashFunction;

/**
 * NonCollidingTier
 * 
 * @author jwu
 *
 */
class NonCollidingTier implements DataStore<byte[], byte[]>
{
    final static Logger _log = Logger.getLogger(NonCollidingTier.class);
    final NonCollidingStore _dataStore;
    
    /**
     * Creates a keyed dynamic DataStore.
     * 
     * @param homeDir                the home directory of DataStore
     * @param initLevel              the initial level when DataStore is created
     * @param entrySize              the redo entry size (i.e., batch size)
     * @param maxEntries             the number of redo entries required for updating the underlying address array
     * @param segmentFileSizeMB      the size of segment file in MB
     * @param segmentFactory         the segment factory
     * @param segmentCompactTrigger  the percentage of segment capacity, which triggers compaction once per segment
     * @param segmentCompactFactor   the load factor of segment, below which a segment is eligible for compaction
     * @param hashLoadFactor         the load factor of the underlying address array (hash table)
     * @param hashFunction           the hash function for mapping keys to indexes
     * @throws Exception             if this dynamic data store cannot be created.
     */
    public NonCollidingTier(File homeDir,
                           int initLevel,
                           int entrySize,
                           int maxEntries,
                           int segmentFileSizeMB,
                           SegmentFactory segmentFactory,
                           double segmentCompactTrigger,
                           double segmentCompactFactor,
                           HashFunction<byte[]> hashFunction) throws Exception
    {
        _dataStore = new NonCollidingStore(
                homeDir,
                initLevel,
                entrySize,
                maxEntries,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactTrigger,
                segmentCompactFactor,
                hashFunction);
        
        _log.info(_dataStore.getStatus());
    }
    
    @Override
    public synchronized void clear() throws IOException
    {
        _dataStore.clear();
    }
    
    @Override
    public synchronized void persist() throws IOException
    {
        _dataStore.persist();
    }
    
    @Override
    public synchronized void sync() throws IOException
    {
        _dataStore.sync();
    }
    
    @Override
    public byte[] get(byte[] key)
    {
        return (key == null) ? null : _dataStore.get(key);
    }
    
    @Override
    public synchronized boolean put(byte[] key, byte[] value) throws Exception
    {
        return (key == null) ? false : ((value == null) ?
                                        _dataStore.delete(key) :
                                        _dataStore.put(key, value));
    }
    
    @Override
    public synchronized boolean delete(byte[] key) throws Exception
    {
        return (key == null) ? false : _dataStore.delete(key);
    }
    
    protected final synchronized boolean put(long hashCode, byte[] key, byte[] value) throws Exception
    {
        return (key == null) ? false : ((value == null) ?
                                        _dataStore.delete(hashCode, key) :
                                        _dataStore.put(hashCode, key, value));
    }
    
    protected final synchronized boolean delete(long hashCode, byte[] key) throws Exception
    {
        return (key == null) ? false : _dataStore.delete(hashCode, key);
    }
    
    protected final String getStatus()
    {
        return _dataStore.getStatus();
    }
    
    protected final void trySplit()
    {
        try
        {
            _dataStore.trySplit();
        }
        catch(Exception e)
        {
            _log.error("Failed to trySplit", e);
        }
    }
    
    protected final void forceSplit()
    {
        try
        {
            _dataStore.forceSplit();
        }
        catch(Exception e)
        {
            _log.error("Failed to forceSplit", e);
        }
    }
    
    protected final void rehash()
    {
        try
        {
            _dataStore.rehash();
        }
        catch(Exception e)
        {
            _log.error("Failed to rehash", e);
        }
    }
}
