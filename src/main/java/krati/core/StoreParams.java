package krati.core;

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
 */
public class StoreParams {
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
     * The minimum segment load threshold is 0.25, below which a segment is eligible for compaction.
     */
    public static final double SEGMENT_COMPACT_FACTOR_MIN = 0.25;
    
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
    
    private int _batchSize = BATCH_SIZE_DEFAULT;
    private int _numSyncBatches = NUM_SYNC_BATCHES_DEFAULT;
    private int _segmentFileSizeMB = SEGMENT_FILE_SIZE_MB_DEFAULT;
    private double _segmentCompactFactor = SEGMENT_COMPACT_FACTOR_DEFAULT;
    private double _hashLoadFactor = HASH_LOAD_FACTOR_DEFAULT;
    
    public void setBatchSize(int batchSize) {
        this._batchSize = batchSize;
    }
    
    public int getBatchSize() {
        return _batchSize;
    }
    
    public void setNumSyncBatches(int numSyncBatches) {
        this._numSyncBatches = numSyncBatches;
    }
    
    public int getNumSyncBatches() {
        return _numSyncBatches;
    }
    
    public void setSegmentFileSizeMB(int segmentFileSizeMB) {
        this._segmentFileSizeMB = segmentFileSizeMB;
    }

    public int getSegmentFileSizeMB() {
        return _segmentFileSizeMB;
    }
    
    public void setSegmentCompactFactor(double segmentCompactFactor) {
        this._segmentCompactFactor = segmentCompactFactor;
    }
    
    public double getSegmentCompactFactor() {
        return _segmentCompactFactor;
    }
    
    public void setHashLoadFactor(double hashLoadFactor) {
        this._hashLoadFactor = hashLoadFactor;
    }
    
    public double getHashLoadFactor() {
        return _hashLoadFactor;
    }
    
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
