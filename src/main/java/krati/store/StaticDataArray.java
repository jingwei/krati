/*
 * Copyright (c) 2010-2011 LinkedIn, Inc
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

package krati.store;

import java.io.File;
import java.io.IOException;

import krati.core.StoreParams;
import org.apache.log4j.Logger;

import krati.core.StoreConfig;
import krati.core.array.AddressArray;
import krati.core.array.AddressArrayFactory;
import krati.core.segment.Segment;
import krati.core.segment.SegmentFactory;

/**
 * StaticDataArray - a convenient class for creating a fixed-size data array.
 * 
 * @author jwu
 * @since 09/24, 2010
 * 
 * <p>
 * 06/25, 2011 - Added constructor using StoreConfig <br/>
 */
public final class StaticDataArray extends AbstractDataArray implements ArrayStore {
    private final static Logger _log = Logger.getLogger(StaticDataArray.class);
    
    /**
     * Constructs a static data array. 
     * 
     * @param config - ArrayStore configuration
     * @throws Exception if the store can not be created.
     */
    public StaticDataArray(StoreConfig config) throws Exception {
        super(config);
    }
    
    /**
     * Constructs a static data array with the following default params.
     * 
     * <pre>
     *   batchSize            : 10000
     *   numSyncBatches       : 5
     *   segmentCompactFactor : 0.5
     *   segmentFileSizeMB    : 256
     * </pre>
     * 
     * @param length            - the array length
     * @param homeDirectory     - the home directory of data array
     * @param segmentFactory    - the segment factory
     * @throws Exception
     */
    public StaticDataArray(int length,
                           File homeDirectory,
                           SegmentFactory segmentFactory) throws Exception {
        this(length,
             StoreParams.BATCH_SIZE_DEFAULT,
             StoreParams.NUM_SYNC_BATCHES_DEFAULT,
             homeDirectory,
             segmentFactory,
             Segment.defaultSegmentFileSizeMB,
             Segment.defaultSegmentCompactFactor);
    }
    
    /**
     * Constructs a static data array with the following default params.
     * 
     * <pre>
     *   batchSize            : 10000
     *   numSyncBatches       : 5
     *   segmentCompactFactor : 0.5
     * </pre>
     * 
     * @param length            - the array length
     * @param homeDirectory     - the home directory of data array
     * @param segmentFactory    - the segment factory
     * @param segmentFileSizeMB - the segment size in MB
     * @throws Exception
     */
    public StaticDataArray(int length,
                           File homeDirectory,
                           SegmentFactory segmentFactory,
                           int segmentFileSizeMB) throws Exception {
        this(length,
             StoreParams.BATCH_SIZE_DEFAULT,
             StoreParams.NUM_SYNC_BATCHES_DEFAULT,
             homeDirectory,
             segmentFactory,
             segmentFileSizeMB,
             StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT);
    }
    
    /**
     * Constructs a static data array.
     * 
     * @param length               - the array length
     * @param batchSize            - the number of updates per update batch
     * @param numSyncBatches       - the number of update batches required for updating <code>indexes.dat</code>
     * @param homeDirectory        - the home directory of data array
     * @param segmentFactory       - the segment factory
     * @param segmentFileSizeMB    - the segment size in MB
     * @param segmentCompactFactor - the segment load threshold, below which a segment is eligible for compaction
     * @throws Exception
     */
    public StaticDataArray(int length,
                           int batchSize,
                           int numSyncBatches,
                           File homeDirectory,
                           SegmentFactory segmentFactory,
                           int segmentFileSizeMB,
                           double segmentCompactFactor) throws Exception {
        super(length,
              batchSize,
              numSyncBatches,
              homeDirectory,
              segmentFactory,
              segmentFileSizeMB,
              segmentCompactFactor);
        _log.info("init " + getStatus());
    }
    
    @Override
    protected AddressArray createAddressArray(File homeDir,
                                              int length,
                                              int batchSize,
                                              int numSyncBatches,
                                              boolean indexesCached) throws Exception {
        AddressArrayFactory factory = new AddressArrayFactory(indexesCached);
        AddressArray addrArray = factory.createStaticAddressArray(homeDir, length, batchSize, numSyncBatches);
        
        if(length != addrArray.length()) {
            _log.warn("array file length " + addrArray.length() + " is different from specified " + length);
        }
        
        return addrArray;
    }
    
    @Override
    public synchronized void delete(int index, long scn) throws Exception {
        if(hasIndex(index)) {
            _dataArray.set(index, null, scn);
        }
    }
    
    @Override
    public final int getIndexStart() {
        return 0;
    }
    
    @Override
    public final int capacity() {
        return length();
    }
    
    @Override
    public final boolean isOpen() {
        return _dataArray.isOpen();
    }
    
    @Override
    public synchronized void open() throws IOException {
        _dataArray.open();
    }
    
    @Override
    public synchronized void close() throws IOException {
        _dataArray.close();
    }
}
