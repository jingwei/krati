package krati.core;

import krati.core.array.basic.DynamicConstants;
import krati.core.segment.Segment;

/**
 * StoreParams
 * 
 * @author jwu
 * 06/09, 2011
 * 
 */
public class StoreParams {
    public static final int BATCH_SIZE_MIN = 1;
    public static final int BATCH_SIZE_DEFAULT = 10000;
    
    public static final int NUM_SYNC_BATCHES_MIN = 1;
    public static final int NUM_SYNC_BATCHES_DEFAULT = 5;

    public static final int SEGMENT_FILE_SIZE_MB_MIN = Segment.minSegmentFileSizeMB;
    public static final int SEGMENT_FILE_SIZE_MB_MAX = Segment.maxSegmentFileSizeMB;
    public static final int SEGMENT_FILE_SIZE_MB_DEFAULT = 128;
    
    public static final double SEGMENT_COMPACT_FACTOR_MIN = 0.25;
    public static final double SEGMENT_COMPACT_FACTOR_MAX = 0.75;
    public static final double SEGMENT_COMPACT_FACTOR_DEFAULT = Segment.defaultSegmentCompactFactor;
    
    public static final double HASH_LOAD_FACTOR_MIN = 0.5;
    public static final double HASH_LOAD_FACTOR_MAX = 1.0;
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
