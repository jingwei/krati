/*
 * Copyright (c) 2010-2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package krati.core;

import java.util.Properties;

import krati.core.array.basic.DynamicConstants;
import krati.core.segment.Segment;
import krati.util.LinearHashing;

/**
 * StoreParams
 * 
 * @author jwu
 * @since 06/09, 2011
 * 
 * <p>
 * 06/12, 2011 - Added JavaDoc comment <br/>
 * 06/22, 2011 - Added new parameter names <br/>
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
    
    /**
     * Whether the indexes array is cached in memory.
     */
    private boolean _indexesCached = INDEXES_CACHED_DEFAULT;
    
    /**
     * The update batch size.
     */
    private int _batchSize = BATCH_SIZE_DEFAULT;
    
    /**
     * The number of update batches of a sync cycle.
     */
    private int _numSyncBatches = NUM_SYNC_BATCHES_DEFAULT;
    
    /**
     * The size of a store segment measured in MB.
     */
    private int _segmentFileSizeMB = SEGMENT_FILE_SIZE_MB_DEFAULT;
    
    /**
     * The segment compaction factor (between 0 and 1), below which a store segment is eligible for being compacted.
     */
    private double _segmentCompactFactor = SEGMENT_COMPACT_FACTOR_DEFAULT;
    
    /**
     * The hash table load factor (between 0 and 1), above which a hash table will be rehashed through a hashing strategy
     * such as Linear Hashing or Extensible Hashing.
     */
    private double _hashLoadFactor = HASH_LOAD_FACTOR_DEFAULT;
    
    /**
     * Creates a new instance of StoreParams.
     */
    protected StoreParams() {
        this.setBatchSize(BATCH_SIZE_DEFAULT);
        this.setNumSyncBatches(NUM_SYNC_BATCHES_DEFAULT);
        this.setSegmentFileSizeMB(SEGMENT_FILE_SIZE_MB_DEFAULT);
        this.setSegmentCompactFactor(SEGMENT_COMPACT_FACTOR_DEFAULT);
        this.setHashLoadFactor(HASH_LOAD_FACTOR_DEFAULT);
        this.setIndexesCached(INDEXES_CACHED_DEFAULT);
    }
    
    /**
     * Sets the size of update batch.
     */
    public void setBatchSize(int batchSize) {
        this._batchSize = batchSize;
        this._properties.setProperty(PARAM_BATCH_SIZE, _batchSize+"");
    }
    
    /**
     * Gets the size of update batch.
     */
    public int getBatchSize() {
        return _batchSize;
    }
    
    /**
     * Sets the number of update batches of a sync cycle.
     */
    public void setNumSyncBatches(int numSyncBatches) {
        this._numSyncBatches = numSyncBatches;
        this._properties.setProperty(PARAM_NUM_SYNC_BATCHES, _numSyncBatches+"");
    }
    
    /**
     * Gets the number of update batches of a sync cycle.
     */
    public int getNumSyncBatches() {
        return _numSyncBatches;
    }
    
    /**
     * Sets the size of store segments in MB.
     */
    public void setSegmentFileSizeMB(int segmentFileSizeMB) {
        this._segmentFileSizeMB = segmentFileSizeMB;
        this._properties.setProperty(PARAM_SEGMENT_FILE_SIZE_MB, _segmentFileSizeMB+"");
    }
    
    /**
     * Gets the size of store segments in MB.
     */
    public int getSegmentFileSizeMB() {
        return _segmentFileSizeMB;
    }
    
    /**
     * Sets the segment compaction factor (between 0 and 1),
     * below which a store segment is eligible for being compacted.
     *  
     * @param segmentCompactFactor - the segment compaction factor
     */
    public void setSegmentCompactFactor(double segmentCompactFactor) {
        this._segmentCompactFactor = segmentCompactFactor;
        this._properties.setProperty(PARAM_SEGMENT_COMPACT_FACTOR, _segmentCompactFactor+"");
    }
    
    /**
     * Gets the segment compaction factor.
     */
    public double getSegmentCompactFactor() {
        return _segmentCompactFactor;
    }
    
    /**
     * Sets the hash table load factor (between 0 and 1), above which a hash table will be
     * rehashed through a hashing strategy such as Linear Hashing or Extensible Hashing.
     * 
     * @param hashLoadFactor - the hash table load factor
     */
    public void setHashLoadFactor(double hashLoadFactor) {
        this._hashLoadFactor = hashLoadFactor;
        this._properties.setProperty(PARAM_HASH_LOAD_FACTOR, _hashLoadFactor+"");
    }
    
    /**
     * Gets the hash table load factor.
     */
    public double getHashLoadFactor() {
        return _hashLoadFactor;
    }
    
    /**
     * Sets the boolean value indicating whether the indexes (i.e. indexes.dat) is cached in memory or not.
     */
    public void setIndexesCached(boolean b) {
        this._indexesCached = b;
        this._properties.setProperty(PARAM_INDEXES_CACHED, _indexesCached ? "true" : "false");
    }
    
    /**
     * Gets the boolean value indicating whether the indexes (i.e. indexes.dat) is cached in memory or not.
     */
    public boolean getIndexesCached() {
        return _indexesCached;
    }
    
    /**
     * Tests whether the indexes (i.e. indexes.dat) is cached in memory or not.
     */
    public boolean isIndexesCached() {
        return _indexesCached;
    }
    
    /**
     * Parameter for specifying the indexes (i.e. indexes.dat) cached in memory.
     * The value is <code>true</code> or <code>false</code>.
     */
    public static final String PARAM_INDEXES_CACHED         = "krati.store.indexes.cached";
    
    /**
     * Parameter for specifying the indexes update batch size.
     */
    public static final String PARAM_BATCH_SIZE             = "krati.store.batchSize";
    
    /**
     * Parameter for specifying the number of batches required for updating the underlying indexes.
     */
    public static final String PARAM_NUM_SYNC_BATCHES       = "krati.store.numSyncBatches";
    
    /**
     * Parameter for specifying the store segment file size in MB.
     */
    public static final String PARAM_SEGMENT_FILE_SIZE_MB   = "krati.store.segment.file.size";
    
    /**
     * Parameter for specifying the store segment compactor factor between 0 and 0.75.
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
     * Parameter for specifying the start index of a array store partition.
     */
    public static final String PARAM_PARTITION_START        = "krati.store.partition.start";
    
    /**
     * Parameter for specifying the count (i.e. capacity) of a array store partition.
     */
    public static final String PARAM_PARTITION_COUNT        = "krati.store.partition.count";
    
    /**
     * Parameter for specifying the data handler class of a data store.
     */
    public static final String PARAM_DATA_HANDLER_CLASS     = "krati.store.data.handler.class";
    
    /**
     * Gets the initial level of {@link krati.store.DynamicDataStore DynamicDataStore}, {@link krati.store.DynamicDataSet DynamicDataSet}
     * and {@link krati.store.IndexedDataStore IndexedDataStore} based on the initial store capacity.
     * The returned initial <code>level</code> is the minimum integer which satisfies the condition
     * <code>initialCapacity</code> less than or equal to <code>2 ^ (16 + level)</code>.
     * 
     * @param initialCapacity - the initial store capacity
     * @return an initial level which satisfies the condition
     * <code>initialCapacity</code> less than or equal to <code>2 ^ (16 + level)</code>.
     */
    public static final int getDynamicStoreInitialLevel(int initialCapacity) {
        if (initialCapacity <= DynamicConstants.SUB_ARRAY_SIZE) {
            return 0;
        } else {
            double d = (double)initialCapacity / (double)DynamicConstants.SUB_ARRAY_SIZE;
            int level = (int)Math.ceil(Math.log(d)/Math.log(2));
            return level;
        }
    }
    
    /**
     * Gets the initial capacity of {@link krati.store.DynamicDataStore DynamicDataStore}, {@link krati.store.DynamicDataSet DynamicDataSet}
     * and {@link krati.store.IndexedDataStore IndexedDataStore} based on the <code>initialLevel</code> of store.
     * The <code>initialLevel</code> determines that the <code>initialCapacity</code> should be
     * equal to <code>2 ^ (16 + level)</code> and less than <code>Integer.MAX_VALUE</code>.
     * 
     * @param initialLevel - the initial level of store, which should be in the range [0, 15).
     * @return the initial capacity of a dynamic store.
     */
    public static final int getDynamicStoreInitialCapacity(int initialLevel) {
        // Compute maxLevel
        LinearHashing h = new LinearHashing(DynamicConstants.SUB_ARRAY_SIZE);
        h.reinit(Integer.MAX_VALUE);
        int maxLevel = h.getLevel();
        
        // Compute initialCapacity
        int initialCapacity = DynamicConstants.SUB_ARRAY_SIZE;
        if(initialLevel > 0) {
            if(initialLevel > maxLevel) {
                initialLevel = maxLevel;
            }
            initialCapacity = DynamicConstants.SUB_ARRAY_SIZE << initialLevel;
        }
        
        return initialCapacity;
    }
}
