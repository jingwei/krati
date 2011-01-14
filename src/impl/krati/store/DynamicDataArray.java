package krati.store;

import java.io.File;

import krati.array.DynamicArray;
import krati.core.array.AddressArray;
import krati.core.array.basic.DynamicLongArray;
import krati.core.segment.Segment;
import krati.core.segment.SegmentFactory;

import org.apache.log4j.Logger;

/**
 * DynamicDataArray - a convenient class for creating a dynamic data array.
 * 
 * @author jwu
 * Sep 24, 2010
 */
public final class DynamicDataArray extends AbstractDataArray implements DynamicArray, ArrayStore {
    private final static Logger _log = Logger.getLogger(DynamicDataArray.class);
    
    /**
     * Constructs a dynamic data array with the following default params.
     * 
     * <pre>
     *   batchSize            : 10000
     *   numSyncBatches       : 5
     *   segmentCompactFactor : 0.5
     *   segmentFileSizeMB    : 256
     * </pre>
     * 
     * @param initialLength   - the initial array length
     * @param homeDirectory   - the home directory of data array
     * @param segmentFactory  - the segment factory
     * @throws Exception
     */
    public DynamicDataArray(int initialLength,
                            File homeDirectory,
                            SegmentFactory segmentFactory) throws Exception {
        this(initialLength,
             10000,
             5,
             homeDirectory,
             segmentFactory,
             Segment.defaultSegmentFileSizeMB,
             Segment.defaultSegmentCompactFactor);
    }
    
    /**
     * Constructs a dynamic data array with the following default params.
     * 
     * <pre>
     *   batchSize              : 10000
     *   numSyncBatches         : 5
     *   segmentCompactFactor   : 0.5
     * </pre>
     * 
     * @param initialLength     - the initial array length
     * @param homeDirectory     - the home directory of data array
     * @param segmentFactory    - the segment factory
     * @param segmentFileSizeMB - the segment size in MB
     * @throws Exception
     */
    public DynamicDataArray(int initialLength,
                            File homeDirectory,
                            SegmentFactory segmentFactory,
                            int segmentFileSizeMB) throws Exception {
        this(initialLength,
             10000,
             5,
             homeDirectory,
             segmentFactory,
             segmentFileSizeMB,
             0.5);
    }
    
    /**
     * Constructs a dynamic data array.
     * 
     * @param initialLength        - the initial array length
     * @param batchSize            - the number of updates per update batch, i.e. the redo entry size
     * @param numSyncBatches       - the number of update batches required for updating the underlying address array
     * @param homeDirectory        - the home directory of data array
     * @param segmentFactory       - the segment factory
     * @param segmentFileSizeMB    - the segment size in MB
     * @param segmentCompactFactor - the segment load threshold, below which a segment is eligible for compaction
     * @throws Exception
     */
    public DynamicDataArray(int initialLength,
                            int batchSize,
                            int numSyncBatches,
                            File homeDirectory,
                            SegmentFactory segmentFactory,
                            int segmentFileSizeMB,
                            double segmentCompactFactor) throws Exception {
        super(initialLength,
              batchSize,
              numSyncBatches,
              homeDirectory,
              segmentFactory,
              segmentFileSizeMB,
              segmentCompactFactor);
        _log.info("init " + getStatus());
    }
    
    @Override
    protected AddressArray createAddressArray(int length,
                                              int batchSize,
                                              int numSyncBatches,
                                              File homeDirectory) throws Exception {
        DynamicLongArray addrArray;
        addrArray = new DynamicLongArray(batchSize, numSyncBatches, homeDirectory);
        addrArray.expandCapacity(length - 1);
        
        if(length != addrArray.length()) {
            _log.warn("array file length " + addrArray.length() + " is different from specified " + length);
        }
        
        return addrArray;
    }
    
    @Override
    public synchronized void expandCapacity(int index) throws Exception {
        ((DynamicLongArray)_addrArray).expandCapacity(index);
    }
    
    @Override
    public synchronized void set(int index, byte[] data, long scn) throws Exception {
        ((DynamicLongArray)_addrArray).expandCapacity(index);
        _dataArray.set(index, data, scn);
    }
    
    @Override
    public synchronized void set(int index, byte[] data, int offset, int length, long scn) throws Exception {
        ((DynamicLongArray)_addrArray).expandCapacity(index);
        _dataArray.set(index, data, offset, length, scn);
    }

    @Override
    public int capacity() {
        return length();
    }
    
    @Override
    public synchronized void delete(int index, long scn) throws Exception {
        if(hasIndex(index)) {
            _dataArray.set(index, null, scn);
        }
    }
}
