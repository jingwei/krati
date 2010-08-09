package krati.cds.impl.store.tier;

import java.io.File;

import krati.cds.impl.segment.SegmentFactory;
import krati.cds.impl.store.DynamicDataStore;
import krati.util.HashFunction;

/**
 * CollidingStore
 * 
 * @author jwu
 *
 */
class CollidingStore extends DynamicDataStore
{
    private final TieredDataStore _mainStore;
    
    /**
     * Creates a dynamic DataStore.
     * 
     * @param mainStore              the main TieredDataStore
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
    public CollidingStore(TieredDataStore mainStore,
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
        super(homeDir,
              initLevel,
              entrySize,
              maxEntries,
              segmentFileSizeMB,
              segmentFactory,
              segmentCompactTrigger,
              segmentCompactFactor,
              hashLoadFactor,
              hashFunction);
        
        _mainStore = mainStore;
    }
    
    @Override
    protected synchronized void split() throws Exception
    {
        if(_mainStore != null && getSplit() == 0)
        {
            _mainStore.startSplitNonCollidingTiers();
        }
        
        super.split();
        
        if(_mainStore != null && getSplit() == 0)
        {
            _mainStore.splitDone();
        }
    }
    
    protected final void trySplit() throws Exception
    {
        if(getSplit() > 0)
        {
            split();
        }
    }
}
