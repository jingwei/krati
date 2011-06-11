package krati.core;

import java.io.File;

import krati.core.segment.SegmentFactory;
import krati.store.ArrayStore;
import krati.store.DynamicDataArray;
import krati.store.DynamicDataStore;
import krati.store.IndexedDataStore;
import krati.store.StaticDataArray;
import krati.store.StaticDataStore;
import krati.util.FnvHashFunction;

/**
 * StoreFactory offers a standard API for creating different stores including
 * {@link krati.store.ArrayStore ArrayStore} and {@link krati.store.DataStore DataStore}.
 * 
 * @author jwu
 * 06/09, 2011
 * 
 */
public class StoreFactory {
    
    public static ArrayStore createStaticArrayStore(
            File homeDir,
            int length,
            int segmentFileSizeMB,
            SegmentFactory segmentFactory) throws Exception {
        int batchSize = StoreParams.BATCH_SIZE_DEFAULT;
        int numSyncBatches = StoreParams.NUM_SYNC_BATCHES_DEFAULT;
        double segmentCompactFactor = StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT;
        
        return createStaticArrayStore(
                homeDir,
                length,
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactFactor);
    }
    
    public static ArrayStore createStaticArrayStore(
            File homeDir,
            int length,
            int batchSize,
            int numSyncBatches,
            int segmentFileSizeMB,
            SegmentFactory segmentFactory) throws Exception {
        double segmentCompactFactor = StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT;
        
        return createStaticArrayStore(
                homeDir,
                length,
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactFactor);
    }
    
    public static ArrayStore createStaticArrayStore(
            File homeDir,
            int length,
            int batchSize,
            int numSyncBatches,
            int segmentFileSizeMB,
            SegmentFactory segmentFactory,
            double segmentCompactFactor) throws Exception {
        return new StaticDataArray(
                length,
                batchSize,
                numSyncBatches,
                homeDir,
                segmentFactory,
                segmentFileSizeMB,
                segmentCompactFactor);
    }
    
    public static ArrayStore createDynamicArrayStore(
            File homeDir,
            int length,
            int segmentFileSizeMB,
            SegmentFactory segmentFactory) throws Exception {
        int batchSize = StoreParams.BATCH_SIZE_DEFAULT;
        int numSyncBatches = StoreParams.NUM_SYNC_BATCHES_DEFAULT;
        double segmentCompactFactor = StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT;
        
        return createDynamicArrayStore(
                homeDir,
                length,
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactFactor);
    }
    
    public static ArrayStore createDynamicArrayStore(
            File homeDir,
            int initialLength,
            int batchSize,
            int numSyncBatches,
            int segmentFileSizeMB,
            SegmentFactory segmentFactory) throws Exception {
        double segmentCompactFactor = StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT;
        
        return createDynamicArrayStore(
                homeDir,
                initialLength,
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactFactor);
    }
    
    public static ArrayStore createDynamicArrayStore(
            File homeDir,
            int initialLength,
            int batchSize,
            int numSyncBatches,
            int segmentFileSizeMB,
            SegmentFactory segmentFactory,
            double segmentCompactFactor) throws Exception {
        return new DynamicDataArray(
                initialLength,
                batchSize,
                numSyncBatches,
                homeDir,
                segmentFactory,
                segmentFileSizeMB,
                segmentCompactFactor);
    }
    
    public static StaticDataStore createStaticDataStore(
            File homeDir,
            int capacity,
            int segmentFileSizeMB,
            SegmentFactory segmentFactory) throws Exception {
        int batchSize = StoreParams.BATCH_SIZE_DEFAULT;
        int numSyncBatches = StoreParams.NUM_SYNC_BATCHES_DEFAULT;
        double segmentCompactFactor = StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT;
        
        return createStaticDataStore(
                homeDir,
                capacity,
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactFactor);
    }
    
    public static StaticDataStore createStaticDataStore(
            File homeDir,
            int capacity,
            int batchSize,
            int numSyncBatches,
            int segmentFileSizeMB,
            SegmentFactory segmentFactory) throws Exception {
        double segmentCompactFactor = StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT;
        
        return createStaticDataStore(
                homeDir,
                capacity,
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactFactor);
    }
    
    public static StaticDataStore createStaticDataStore(
            File homeDir,
            int capacity,
            int batchSize,
            int numSyncBatches,
            int segmentFileSizeMB,
            SegmentFactory segmentFactory,
            double segmentCompactFactor) throws Exception {
        return new StaticDataStore(
                homeDir,
                capacity,
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactFactor,
                new FnvHashFunction());
    }
    
    public static DynamicDataStore createDynamicDataStore(
            File homeDir,
            int initialCapacity,
            int segmentFileSizeMB,
            SegmentFactory segmentFactory) throws Exception {
        int batchSize = StoreParams.BATCH_SIZE_DEFAULT;
        int numSyncBatches = StoreParams.NUM_SYNC_BATCHES_DEFAULT;
        double hashLoadFactor = StoreParams.HASH_LOAD_FACTOR_DEFAULT;
        double segmentCompactFactor = StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT;
        
        return createDynamicDataStore(
                homeDir,
                StoreParams.getDynamicStoreInitialLevel(initialCapacity),
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactFactor,
                hashLoadFactor);
    }
    
    public static DynamicDataStore createDynamicDataStore(
            File homeDir,
            int initialCapacity,
            int batchSize,
            int numSyncBatches,
            int segmentFileSizeMB,
            SegmentFactory segmentFactory) throws Exception {
        double hashLoadFactor = StoreParams.HASH_LOAD_FACTOR_DEFAULT;
        double segmentCompactFactor = StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT;
        
        return createDynamicDataStore(
                homeDir,
                StoreParams.getDynamicStoreInitialLevel(initialCapacity),
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactFactor,
                hashLoadFactor);
    }
    
    public static DynamicDataStore createDynamicDataStore(
            File homeDir,
            int initialCapacity,
            int batchSize,
            int numSyncBatches,
            int segmentFileSizeMB,
            SegmentFactory segmentFactory,
            double segmentCompactFactor) throws Exception {
        double hashLoadFactor = StoreParams.HASH_LOAD_FACTOR_DEFAULT;
        
        return createDynamicDataStore(
                homeDir,
                StoreParams.getDynamicStoreInitialLevel(initialCapacity),
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactFactor,
                hashLoadFactor);
    }
    
    public static DynamicDataStore createDynamicDataStore(
            File homeDir,
            int initialCapacity,
            int batchSize,
            int numSyncBatches,
            int segmentFileSizeMB,
            SegmentFactory segmentFactory,
            double segmentCompactFactor,
            double hashLoadFactor) throws Exception {
        return new DynamicDataStore(
                homeDir,
                StoreParams.getDynamicStoreInitialLevel(initialCapacity),
                batchSize,
                numSyncBatches,
                segmentFileSizeMB,
                segmentFactory,
                segmentCompactFactor,
                hashLoadFactor,
                new FnvHashFunction());
    }
    
    public static IndexedDataStore createIndexedDataStore(
            File homeDir,
            int initialCapacity,
            int indexSegmentFileSizeMB,
            SegmentFactory indexSegmentFactory,
            int storeSegmentFileSizeMB,
            SegmentFactory storeSegmentFactory) throws Exception {
        int batchSize = StoreParams.BATCH_SIZE_DEFAULT;
        int numSyncBatches = StoreParams.NUM_SYNC_BATCHES_DEFAULT;
        
        return createIndexedDataStore(
                homeDir,
                initialCapacity,
                batchSize,
                numSyncBatches,
                indexSegmentFileSizeMB,
                indexSegmentFactory,
                storeSegmentFileSizeMB,
                storeSegmentFactory);
    }
    
    public static IndexedDataStore createIndexedDataStore(
            File homeDir,
            int initialCapacity,
            int batchSize,
            int numSyncBatches,
            int indexSegmentFileSizeMB,
            SegmentFactory indexSegmentFactory,
            int storeSegmentFileSizeMB,
            SegmentFactory storeSegmentFactory) throws Exception {
        int initLevel = StoreParams.getDynamicStoreInitialLevel(initialCapacity);
        
        return new IndexedDataStore(
                homeDir,
                batchSize,
                numSyncBatches,
                initLevel,
                indexSegmentFileSizeMB,
                indexSegmentFactory,
                initLevel >> 1,
                storeSegmentFileSizeMB,
                storeSegmentFactory);
    }
}
