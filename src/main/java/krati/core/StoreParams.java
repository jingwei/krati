package krati.core;

import java.util.Properties;

import krati.core.array.basic.DynamicConstants;
import krati.core.segment.Segment;

/**
 * StoreParams
 * 
 * @author jwu
 * 06/09, 2011
 * 
 * <p>
 * 06/12, 2011 - Added JavaDoc comment
 * 06/22, 2011 - Added new parameter names
 */
public class StoreParams {
    /**
     * Store properties
     */
    protected final Properties _properties = new Properties();
    
    /**
     * The indexes (indexes.dat) is cached in memory by default.
     */
    public static final boolean INDEXES_CACHED_DEFAULT = true;
    
    /**
     * The minimum number of updates per update batch is 1.
     * This value is not recommended due to its inefficiency.
     */
    public static final int BATCH_SIZE_MIN = 1;
    
    /**
     * The default number of updates per update batch is 10000.
     * A value in the range from 1000 to 10000 is recommended.
     */
    public static final int BATCH_SIZE_DEFAULT = 10000;
    
    /**
     * The minimum number of update batches required for updating the underlying indexes is 1.
     * This value is not recommended due to its inefficiency.
     */
    public static final int NUM_SYNC_BATCHES_MIN = 1;
    
    /**
     * The default number of update batches required for updating the underlying indexes is 5.
     * A value in the range from 5 to 10 is recommended.
     */
    public static final int NUM_SYNC_BATCHES_DEFAULT = 5;
    
    /**
     * The minimum segment file size in MB is 8.
     */
    public static final int SEGMENT_FILE_SIZE_MB_MIN = Segment.minSegmentFileSizeMB;
    
    /**
     * The maximum segment file size in MB is 2048.
     */
    public static final int SEGMENT_FILE_SIZE_MB_MAX = Segment.maxSegmentFileSizeMB;
    
    /**
     * The default segment file size in MB is 256 (for backward compatibility).
     * The recommended segment file size in MB is 32, 64 or 128.
     */
    public static final int SEGMENT_FILE_SIZE_MB_DEFAULT = Segment.defaultSegmentFileSizeMB;
    
    /**
     * The minimum segment load threshold is 0 and it effectively disables segment compaction.
     */
    public static final double SEGMENT_COMPACT_FACTOR_MIN = 0;
    
    /**
     * The maximum segment load threshold is 0.75, below which a segment is eligible for compaction.
     */
    public static final double SEGMENT_COMPACT_FACTOR_MAX = 0.75;
    
    /**
     * The default segment load threshold is 0.5, below which a segment is eligible for compaction.
     */
    public static final double SEGMENT_COMPACT_FACTOR_DEFAULT = Segment.defaultSegmentCompactFactor;
    
    /**
     * The minimum load factor of the underlying hash table is 0.5.
     */
    public static final double HASH_LOAD_FACTOR_MIN = 0.5;
    
    /**
     * The maximum load factor of the underlying hash table is 1.0. This effectively disables rehashing.
     */
    public static final double HASH_LOAD_FACTOR_MAX = 1.0;
    
    /**
     * The default load factor of the underlying hash table is 0.75, above which rehashing is triggered automatically.
     */
    public static final double HASH_LOAD_FACTOR_DEFAULT = 0.75;
    
    private boolean _indexesCached = INDEXES_CACHED_DEFAULT;
    private int _batchSize = BATCH_SIZE_DEFAULT;
    private int _numSyncBatches = NUM_SYNC_BATCHES_DEFAULT;
    private int _segmentFileSizeMB = SEGMENT_FILE_SIZE_MB_DEFAULT;
    private double _segmentCompactFactor = SEGMENT_COMPACT_FACTOR_DEFAULT;
    private double _hashLoadFactor = HASH_LOAD_FACTOR_DEFAULT;
    
    protected StoreParams() {
        this.setIndexesCached(INDEXES_CACHED_DEFAULT);
        this.setBatchSize(BATCH_SIZE_DEFAULT);
        this.setNumSyncBatches(NUM_SYNC_BATCHES_DEFAULT);
        this.setSegmentFileSizeMB(SEGMENT_FILE_SIZE_MB_DEFAULT);
        this.setSegmentCompactFactor(SEGMENT_COMPACT_FACTOR_DEFAULT);
        this.setHashLoadFactor(HASH_LOAD_FACTOR_DEFAULT);
    }
    
    public void setBatchSize(int batchSize) {
        this._batchSize = batchSize;
        this._properties.setProperty(PARAM_BATCH_SIZE, _batchSize+"");
    }
    
    public int getBatchSize() {
        return _batchSize;
    }
    
    public void setNumSyncBatches(int numSyncBatches) {
        this._numSyncBatches = numSyncBatches;
        this._properties.setProperty(PARAM_NUM_SYNC_BATCHES, _numSyncBatches+"");
    }
    
    public int getNumSyncBatches() {
        return _numSyncBatches;
    }
    
    public void setSegmentFileSizeMB(int segmentFileSizeMB) {
        this._segmentFileSizeMB = segmentFileSizeMB;
        this._properties.setProperty(PARAM_SEGMENT_FILE_SIZE_MB, _segmentFileSizeMB+"");
    }

    public int getSegmentFileSizeMB() {
        return _segmentFileSizeMB;
    }
    
    public void setSegmentCompactFactor(double segmentCompactFactor) {
        this._segmentCompactFactor = segmentCompactFactor;
        this._properties.setProperty(PARAM_SEGMENT_COMPACT_FACTOR, _segmentCompactFactor+"");
    }
    
    public double getSegmentCompactFactor() {
        return _segmentCompactFactor;
    }
    
    public void setHashLoadFactor(double hashLoadFactor) {
        this._hashLoadFactor = hashLoadFactor;
        this._properties.setProperty(PARAM_HASH_LOAD_FACTOR, _hashLoadFactor+"");
    }
    
    public double getHashLoadFactor() {
        return _hashLoadFactor;
    }
    
    public void setIndexesCached(boolean b) {
        this._indexesCached = b;
        this._properties.setProperty(PARAM_INDEXES_CACHED, _indexesCached ? "true" : "false");
    }
    
    public boolean getIndexesCached() {
        return _indexesCached;
    }
    
    public boolean isIndexesCached() {
        return _indexesCached;
    }
    
    /**
     * Parameter for specifying the indexes (i.e. indexes.dat) cached in memory.
     */
    public static final String PARAM_INDEXES_CACHED         = "krati.indexes.cached";
    
    /**
     * Parameter for specifying the indexes update batch size.
     */
    public static final String PARAM_BATCH_SIZE             = "krati.indexes.batchSize";
    
    /**
     * Parameter for specifying the number of batches required for updating the underlying indexes.
     */
    public static final String PARAM_NUM_SYNC_BATCHES       = "krati.indexes.numSyncBatches";
    
    /**
     * Parameter for specifying the store segment file size in MB.
     */
    public static final String PARAM_SEGMENT_FILE_SIZE_MB   = "krati.store.segment.file.size";
    
    /**
     * Parameter for specifying the store segment compactor factor between 0.25 and 0.75.
     */
    public static final String PARAM_SEGMENT_COMPACT_FACTOR = "krati.store.segment.compact.factor";
    
    /**
     * Parameter for specifying the store segment factory class.
     */
    public static final String PARAM_SEGMENT_FACTORY_CLASS  = "krati.store.segment.factory.class";
    
    /**
     * Parameter for specifying the hash load factor of a dynamic store.
     */
    public static final String PARAM_HASH_LOAD_FACTOR       = "krati.store.hash.load.factor";
    
    /**
     * Parameter for specifying the hash function class of a data store.
     */
    public static final String PARAM_HASH_FUNCTION_CLASS    = "krati.store.hash.function.class";
    
    /**
     * Parameter for specifying the initial capacity of a store.
     */
    public static final String PARAM_INITIAL_CAPACITY       = "krati.store.initial.capacity";
    
    /**
     * Get the initial level of {@link krati.store.DynamicDataStore DynamicDataStore}, {@link krati.store.DynamicDataSet DynamicDataSet}
     * and {@link krati.store.IndexedDataStore IndexedDataStore} based an initial store capacity.
     * The initial <code>level</code> is the minimum integer which satisfies the condition
     * <code>initialCapacity</code> less than or equal to <code>2 ^ (16 + level)</code>.
     * 
     * @param initialCapacity - the initial store capacity
     * @return an initial level which satisfies the condition
     * <code>initialCapacity</code> less than or equal to <code>2 ^ (16 + level)</code>.
     */
    public static int getDynamicStoreInitialLevel(int initialCapacity) {
        if (initialCapacity <= DynamicConstants.SUB_ARRAY_SIZE) {
            return 0;
        } else {
            double d = (double)initialCapacity / (double)DynamicConstants.SUB_ARRAY_SIZE;
            int level = (int)Math.ceil(Math.log(d)/Math.log(2));
            return level;
        }
    }
}
