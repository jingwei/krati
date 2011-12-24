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
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import krati.Mode;
import krati.array.DataArray;
import krati.core.StoreConfig;
import krati.core.StoreParams;
import krati.core.array.AddressArray;
import krati.core.array.AddressArrayFactory;
import krati.core.array.SimpleDataArray;
import krati.core.array.basic.DynamicConstants;
import krati.core.segment.SegmentFactory;
import krati.core.segment.SegmentManager;
import krati.store.DataSet;
import krati.store.DataSetHandler;
import krati.util.FnvHashFunction;
import krati.util.HashFunction;
import krati.util.LinearHashing;

/**
 * DynamicDataSet is implemented using Linear Hashing. Its capacity grows as needed.
 * 
 * The values are stored in the underlying DataArray using the following format:
 * <pre>
 * [count:int][value-length:int][value:bytes][value-length:int][value:bytes]...
 *            +-----------value 1------------+-----------value 2-----------+
 * </pre>
 * 
 * @author jwu
 * 
 * <p>
 * 06/06, 2011 - Added support for Closeable <br/>
 * 06/08, 2011 - Scale to the Integer.MAX_VALUE capacity <br/>
 * 06/25, 2011 - Added constructor using StoreConfig <br/>
 */
public class DynamicDataSet implements DataSet<byte[]> {
    private final static Logger _log = Logger.getLogger(DynamicDataSet.class);
    
    private final File _homeDir;
    private final StoreConfig _config;
    private final AddressArray _addrArray;
    private final SimpleDataArray _dataArray;
    private final DataSetHandler _dataHandler;
    private final HashFunction<byte[]> _hashFunction;
    private final double _loadThreshold;
    private final int _unitCapacity;
    private final int _maxLevel;
    private volatile int _split;
    private volatile int _level;
    private volatile int _levelCapacity;
    private volatile int _loadCount;
    private volatile int _loadCountThreshold;
    
    /**
     * Creates a dynamic DataSet with growing capacity as needed.
     * 
     * @param config - Store configuration
     * @throws Exception if the set can not be created.
     * @throws ClassCastException if the data handler from <tt>config</tt> is not {@link DataSetHandler}.
     */
    public DynamicDataSet(StoreConfig config) throws Exception {
        config.validate();
        config.save();
        
        this._config = config;
        this._homeDir = config.getHomeDir();
        
        // Create data set handler
        _dataHandler = (config.getDataHandler() == null) ?
                new DefaultDataSetHandler() : (DataSetHandler)config.getDataHandler();
        
        // Create dynamic address array
        _addrArray = createAddressArray(
                _config.getHomeDir(),
                _config.getBatchSize(),
                _config.getNumSyncBatches(),
                _config.getIndexesCached());
        _unitCapacity = DynamicConstants.SUB_ARRAY_SIZE;
        
        // Compute maxLevel
        LinearHashing h = new LinearHashing(_unitCapacity);
        h.reinit(Integer.MAX_VALUE);
        _maxLevel = h.getLevel();
        
        int initLevel = StoreParams.getDynamicStoreInitialLevel(_config.getInitialCapacity());
        if(initLevel > _maxLevel) {
            _log.warn("initLevel reset from " + initLevel + " to " + _maxLevel);
            initLevel = _maxLevel;
        }
        _addrArray.expandCapacity((_unitCapacity << initLevel) - 1);
        
        // Create underlying segment manager
        String segmentHome = _homeDir.getCanonicalPath() + File.separator + "segs";
        SegmentManager segmentManager = SegmentManager.getInstance(
                segmentHome,
                _config.getSegmentFactory(),
                _config.getSegmentFileSizeMB());
        
        // Create underlying simple data array
        this._dataArray = new SimpleDataArray(_addrArray, segmentManager, _config.getSegmentCompactFactor());
        this._hashFunction = _config.getHashFunction();
        this._loadThreshold = _config.getHashLoadFactor();
        this._loadCount = scan();
        this.initLinearHashing();
        
        _log.info(getStatus());
    }
    
    /**
     * Creates a dynamic DataSet with the settings below:
     * 
     * <pre>
     *    batchSize              : 10000
     *    numSyncBatches         : 5
     *    segmentFileSizeMB      : 256
     *    segmentCompactFactor   : 0.5
     *    DataSet hashLoadFactor : 0.75
     *    DataSet hashFunction   : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir              the home directory of DataSet
     * @param initLevel            the level for initializing DataSet
     * @param segmentFactory       the segment factory
     * @throws Exception           if this dynamic data set cannot be created.
     */
    public DynamicDataSet(File homeDir,
                          int initLevel,
                          SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             initLevel,
             StoreParams.BATCH_SIZE_DEFAULT,
             StoreParams.NUM_SYNC_BATCHES_DEFAULT,
             StoreParams.SEGMENT_FILE_SIZE_MB_DEFAULT,
             segmentFactory,
             StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT,
             StoreParams.HASH_LOAD_FACTOR_DEFAULT,
             new FnvHashFunction());
    }
    
    /**
     * Creates a dynamic DataSet with the settings below:
     * 
     * <pre>
     *    batchSize              : 10000
     *    numSyncBatches         : 5
     *    segmentFileSizeMB      : 256
     *    segmentCompactFactor   : 0.5
     *    DataSet hashLoadFactor : 0.75
     * </pre>
     * 
     * @param homeDir              the home directory of DataSet
     * @param initLevel            the level for initializing DataSet
     * @param segmentFactory       the segment factory
     * @param hashFunction         the hash function for mapping values to indexes
     * @throws Exception           if this dynamic data set cannot be created.
     */
    public DynamicDataSet(File homeDir,
                          int initLevel,
                          SegmentFactory segmentFactory,
                          HashFunction<byte[]> hashFunction) throws Exception {
        this(homeDir,
             initLevel,
             StoreParams.BATCH_SIZE_DEFAULT,
             StoreParams.NUM_SYNC_BATCHES_DEFAULT,
             StoreParams.SEGMENT_FILE_SIZE_MB_DEFAULT,
             segmentFactory,
             StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT,
             StoreParams.HASH_LOAD_FACTOR_DEFAULT,
             hashFunction);
    }
    
    /**
     * Creates a dynamic DataSet with the settings below:
     * 
     * <pre>
     *    batchSize              : 10000
     *    numSyncBatches         : 5
     *    segmentCompactFactor   : 0.5
     *    DataSet hashLoadFactor : 0.75
     *    DataSet hashFunction   : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir              the home directory of DataSet
     * @param initLevel            the level for initializing DataSet
     * @param segmentFileSizeMB    the size of segment file in MB
     * @param segmentFactory       the segment factory
     * @throws Exception           if this dynamic data set cannot be created.
     */
    public DynamicDataSet(File homeDir,
                          int initLevel,
                          int segmentFileSizeMB,
                          SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             initLevel,
             StoreParams.BATCH_SIZE_DEFAULT,
             StoreParams.NUM_SYNC_BATCHES_DEFAULT,
             segmentFileSizeMB,
             segmentFactory,
             StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT,
             StoreParams.HASH_LOAD_FACTOR_DEFAULT,
             new FnvHashFunction());
    }
    
    /**
     * Creates a dynamic DataSet with the settings below:
     * 
     * <pre>
     *    batchSize              : 10000
     *    numSyncBatches         : 5
     *    segmentCompactFactor   : 0.5
     * </pre>
     * 
     * @param homeDir              the home directory of DataSet
     * @param initLevel            the level for initializing DataSet
     * @param segmentFileSizeMB    the size of segment file in MB
     * @param segmentFactory       the segment factory
     * @param hashLoadThreshold    the load factor of the underlying address array (hash table)
     * @param hashFunction         the hash function for mapping values to indexes
     * @throws Exception           if this dynamic data set cannot be created.
     */
    public DynamicDataSet(File homeDir,
                          int initLevel,
                          int segmentFileSizeMB,
                          SegmentFactory segmentFactory,
                          double hashLoadThreshold,
                          HashFunction<byte[]> hashFunction) throws Exception {
        this(homeDir,
             initLevel,
             StoreParams.BATCH_SIZE_DEFAULT,
             StoreParams.NUM_SYNC_BATCHES_DEFAULT,
             segmentFileSizeMB,
             segmentFactory,
             StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT,
             hashLoadThreshold,
             hashFunction);
    }
    
    /**
     * Creates a dynamic DataSet with the settings below:
     * 
     * <pre>
     *    segmentCompactFactor   : 0.5
     *    DataSet hashLoadFactor : 0.75
     *    DataSet hashFunction   : krati.util.FnvHashFunction
     * </pre>
     * 
     * @param homeDir              the home directory of DataSet
     * @param initLevel            the level for initializing DataSet
     * @param batchSize            the number of updates per update batch
     * @param numSyncBatches       the number of update batches required for updating <code>indexes.dat</code>
     * @param segmentFileSizeMB    the size of segment file in MB
     * @param segmentFactory       the segment factory
     * @throws Exception           if this dynamic data set cannot be created.
     */
    public DynamicDataSet(File homeDir,
                          int initLevel,
                          int batchSize,
                          int numSyncBatches,
                          int segmentFileSizeMB,
                          SegmentFactory segmentFactory) throws Exception {
        this(homeDir,
             initLevel,
             batchSize,
             numSyncBatches,
             segmentFileSizeMB,
             segmentFactory,
             StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT,
             StoreParams.HASH_LOAD_FACTOR_DEFAULT,
             new FnvHashFunction());
    }
    
    /**
     * Creates a dynamic DataSet.
     * 
     * <pre>
     *    segmentCompactFactor   : 0.5
     * </pre>
     * 
     * @param homeDir              the home directory of DataSet
     * @param initLevel            the level for initializing DataSet
     * @param batchSize            the number of updates per update batch
     * @param numSyncBatches       the number of update batches required for updating <code>indexes.dat</code>
     * @param segmentFileSizeMB    the size of segment file in MB
     * @param segmentFactory       the segment factory
     * @param hashLoadFactor       the load factor of the underlying address array (hash table)
     * @param hashFunction         the hash function for mapping values to indexes
     * @throws Exception           if this dynamic data set cannot be created.
     */
    public DynamicDataSet(File homeDir,
                          int initLevel,
                          int batchSize,
                          int numSyncBatches,
                          int segmentFileSizeMB,
                          SegmentFactory segmentFactory,
                          double hashLoadFactor,
                          HashFunction<byte[]> hashFunction) throws Exception {
        this(homeDir,
             initLevel,
             batchSize,
             numSyncBatches,
             segmentFileSizeMB,
             segmentFactory,
             StoreParams.SEGMENT_COMPACT_FACTOR_DEFAULT,
             hashLoadFactor,
             hashFunction);
    }
    
    /**
     * Creates a dynamic DataSet.
     * 
     * @param homeDir              the home directory of DataSet
     * @param initLevel            the initial level for creating DataSet
     * @param batchSize            the number of updates per update batch
     * @param numSyncBatches       the number of update batches required for updating <code>indexes.dat</code>
     * @param segmentFileSizeMB    the size of segment file in MB
     * @param segmentFactory       the segment factory
     * @param segmentCompactFactor the load factor of segment, below which a segment is eligible for compaction
     * @param hashLoadFactor       the load factor of the underlying address array (hash table)
     * @param hashFunction         the hash function for mapping values to indexes
     * @throws Exception           if this dynamic data set cannot be created.
     */
    public DynamicDataSet(File homeDir,
                          int initLevel,
                          int batchSize,
                          int numSyncBatches,
                          int segmentFileSizeMB,
                          SegmentFactory segmentFactory,
                          double segmentCompactFactor,
                          double hashLoadFactor,
                          HashFunction<byte[]> hashFunction) throws Exception {
        // Compute maxLevel
        LinearHashing h = new LinearHashing(DynamicConstants.SUB_ARRAY_SIZE);
        h.reinit(Integer.MAX_VALUE);
        _maxLevel = h.getLevel();
        
        // Compute initialCapacity
        int initialCapacity = DynamicConstants.SUB_ARRAY_SIZE;
        if(initLevel >= 0) {
            if(initLevel > _maxLevel) {
                _log.warn("initLevel reset from " + initLevel + " to " + _maxLevel);
                initLevel = _maxLevel;
            }
            initialCapacity = DynamicConstants.SUB_ARRAY_SIZE << initLevel;
        } else {
            _log.warn("initLevel ignored: " + initLevel);
        }
        
        // Set homeDir
        this._homeDir = homeDir;
        
        // Create/validate/store config
        _config = new StoreConfig(_homeDir, initialCapacity);
        _config.setBatchSize(batchSize);
        _config.setNumSyncBatches(numSyncBatches);
        _config.setSegmentFileSizeMB(segmentFileSizeMB);
        _config.setSegmentCompactFactor(segmentCompactFactor);
        _config.setSegmentFactory(segmentFactory);
        _config.setHashLoadFactor(hashLoadFactor);
        _config.setHashFunction(hashFunction);
        _config.validate();
        _config.save();
        
        // Create data set handler
        _dataHandler = new DefaultDataSetHandler();
        
        // Create dynamic address array
        _addrArray = createAddressArray(
                _config.getHomeDir(),
                _config.getBatchSize(),
                _config.getNumSyncBatches(),
                _config.getIndexesCached());
        _addrArray.expandCapacity(initialCapacity - 1);
        _unitCapacity = DynamicConstants.SUB_ARRAY_SIZE;
        
        // Create underlying segment manager
        String segmentHome = homeDir.getCanonicalPath() + File.separator + "segs";
        SegmentManager segmentManager = SegmentManager.getInstance(
                segmentHome,
                _config.getSegmentFactory(),
                _config.getSegmentFileSizeMB());
        
        // Create underlying simple data array
        this._dataArray = new SimpleDataArray(_addrArray, segmentManager, _config.getSegmentCompactFactor());
        this._hashFunction = hashFunction;
        this._loadThreshold = hashLoadFactor;
        this._loadCount = scan();
        this.initLinearHashing();
        
        _log.info(getStatus());
    }
    
    protected AddressArray createAddressArray(File homeDir,
                                              int batchSize,
                                              int numSyncBatches,
                                              boolean indexesCached) throws Exception {
        AddressArrayFactory factory = new AddressArrayFactory(indexesCached);
        AddressArray addrArray = factory.createDynamicAddressArray(homeDir, batchSize, numSyncBatches);
        return addrArray;
    }
    
    protected long hash(byte[] value) {
        return _hashFunction.hash(value);
    }
    
    protected long nextScn() {
        return System.currentTimeMillis();
    }
    
    @Override
    public final int capacity() {
        return _dataArray.length();
    }
    
    @Override
    public synchronized void sync() throws IOException {
        _dataArray.sync();
    }
    
    @Override
    public synchronized void persist() throws IOException {
        _dataArray.persist();
    }
    
    @Override
    public boolean has(byte[] value) {
        if(value == null) return false;
        
        byte[] existingData;
        long hashCode = hash(value);
        
        /**
         * Need SPIN to retrieve data from the underlying array
         * because the index might have changed with the _split. 
         */
        
        // Map value to an array index
        int index = getIndex(hashCode);
        
        do {
            // Read existing data at the index
            existingData = _dataArray.get(index);
            
            // Check that value is still mapped to the known index
            int indexNew = getIndex(hashCode);
            if(index == indexNew) break;
            else index = indexNew;
        } while(true);
        
        return existingData != null && _dataHandler.find(value, existingData);
    }
    
    public final int countCollisions(byte[] value) {
        byte[] existingData;
        long hashCode = hash(value);
        
        /**
         * Need SPIN to retrieve data from the underlying array
         * because the index might have changed with the _split. 
         */
        
        // Map value to an array index
        int index = getIndex(hashCode);
        
        do {
            // Read existing data at the index
            existingData = _dataArray.get(index);
            
            // Check that value is still mapped to the known index
            int indexNew = getIndex(hashCode);
            if(index == indexNew) break;
            else index = indexNew;
        } while(true);
        
        return existingData == null ? 0 : _dataHandler.countCollisions(value, existingData);
    }

    public final boolean hasWithoutCollisions(byte[] value) {
        return countCollisions(value) == 1;
    }
    
    @Override
    public synchronized boolean add(byte[] value) throws Exception {
        if(value == null) return false;
        
        if(canSplit()) {
            split();
        }
        
        int index = getIndex(value);
        return addInternal(index, value);
    }
    
    @Override
    public synchronized boolean delete(byte[] value) throws Exception {
        if(value == null) return false;
        
        if(canSplit()) {
            split();
        }
        
        int index = getIndex(value);
        return deleteInternal(index, value);
    }
    
    @Override
    public synchronized void clear() throws IOException {
        if(_dataArray.isOpen()) {
            _dataArray.clear();
            _loadCount = 0;
        }
    }
    
    protected final int getIndex(byte[] value) {
        long hashCode = hash(value);
        long capacity = _levelCapacity;
        int index = (int)(hashCode % capacity);
        if (index < 0) index = -index;
        
        if (index < _split) {
            capacity = capacity << 1;
            index = (int)(hashCode % capacity);
            if (index < 0) index = -index;
        }
        
        return index;
    }
    
    protected final int getIndex(long hashCode) {
        long capacity = _levelCapacity;
        int index = (int)(hashCode % capacity);
        if (index < 0) index = -index;
        
        if (index < _split) {
            capacity = capacity << 1;
            index = (int)(hashCode % capacity);
            if (index < 0) index = -index;
        }
        
        return index;
    }
    
    protected boolean addInternal(int index, byte[] value) throws Exception {
        byte[] existingData = _dataArray.get(index);
        
        try {
            if(existingData == null || existingData.length == 0) {
                _dataArray.set(index, _dataHandler.assemble(value), nextScn());
                _loadCount++;
            } else {
                if(!_dataHandler.find(value, existingData)) {
                    _dataArray.set(index, _dataHandler.assemble(value, existingData), nextScn());
                }
            }
        } catch (Exception e) {
            _log.warn("Value reset at index="+ index + " value=\"" + new String(value) + "\"", e);
            _dataArray.set(index, _dataHandler.assemble(value), nextScn());
        }
        
        return true;
    }
    
    protected boolean deleteInternal(int index, byte[] value) throws Exception {
        try {
            byte[] existingData = _dataArray.get(index);
            if(existingData != null) {
               int newLength = _dataHandler.remove(value, existingData);
               if(newLength == 0) {
                   // entire data is removed
                   _dataArray.set(index, null, nextScn());
                   _loadCount--;
                   return true;
               } else if(newLength < existingData.length) {
                   // partial data is removed
                   _dataArray.set(index, existingData, 0, newLength, nextScn());
                   return true;
               }
               // else no data is removed
            }
        } catch (Exception e) {
            _log.warn("Failed to delete value=\""+ new String(value) + "\"", e);
            _dataArray.set(index, null, nextScn());
        }
        
        // no data is removed
        return false;
    }
    
    public final int getLevel() {
        return _level;
    }
    
    public final int getSplit() {
        return _split;
    }

    public final int getCapacity() {
        return _dataArray.length();
    }
    
    public final int getUnitCapacity() {
        return _unitCapacity;
    }
    
    public final int getLevelCapacity() {
        return _levelCapacity;
    }
    
    public final int getLoadCount() {
        return _loadCount;
    }
    
    public final double getLoadFactor() {
        return _loadCount / (double)getCapacity();
    }
    
    public final double getLoadThreshold() {
        return _loadThreshold;
    }
    
    protected void initLinearHashing() throws Exception {
        int unitCount = getCapacity() / getUnitCapacity();
        
        if (unitCount <= 1) {
            _level = 0;
            _split = 0;
            _levelCapacity = getUnitCapacity();
            _loadCountThreshold = (int)(getCapacity() * _loadThreshold);
        } else {
            // Determine level and split
            _level = 0;
            int remainder = (unitCount - 1) >> 1;
            while(remainder > 0) {
                _level++;
                remainder = remainder >> 1;
            }
            
            _split = (unitCount - (1 << _level) - 1) * getUnitCapacity();
            _levelCapacity = getUnitCapacity() * (1 << _level);
            _loadCountThreshold = (int)(getCapacity() * _loadThreshold);
            
            // Need to re-populate the last unit
            while(canSplit()) {
                split();
            }
        }
    }
    
    protected boolean canSplit() {
        if(0 < _split || _loadCountThreshold < _loadCount) {
            // The splitTo must NOT overflow Integer.MAX_VALUE
            int splitTo = _levelCapacity + _split;
            if (Integer.MAX_VALUE > splitTo && splitTo >= _levelCapacity) {
                return true;
            }
        }
        
        return false;
    }
    
    protected void split() throws Exception {
        // Ensure address capacity
        _addrArray.expandCapacity(_split + _levelCapacity);
        
        // Read data from the _split index
        byte[] data = _dataArray.get(_split);
        
        // Process read data
        if (data != null && data.length > 0) {
            ByteBuffer bb = ByteBuffer.wrap(data);
            long newCapacity = ((long)_levelCapacity) << 1;
            
            int cnt = bb.getInt();
            while(cnt > 0) {
                // Read value
                int len = bb.getInt();
                byte[] value = new byte[len];
                bb.get(value);
                
                int newIndex = (int)(hash(value) % newCapacity);
                if (newIndex < 0) newIndex = -newIndex;
                
                if(newIndex != _split) {
                    // Remove at the old index
                    deleteInternal(_split, value);
                    
                    // Update at the new index
                    addInternal(newIndex, value);
                }
                
                cnt--;
            }
        }
        
        _split++;

        if(_split % _unitCapacity == 0) {
            _log.info("split " + getStatus());
        }
        
        if(_split == _levelCapacity) {
            int nextLevel = _level + 1;
            int nextLevelCapacity = getUnitCapacity() * (1 << nextLevel);
            if (nextLevelCapacity > _levelCapacity) {
                _split = 0;
                _level = nextLevel;
                _levelCapacity = nextLevelCapacity;
                _loadCountThreshold = (int)(getCapacity() * _loadThreshold);
                _log.info(getStatus());
            } else {
                /* NOT FEASIBLE!
                 * This because canSplit() and split() are paired together
                 */
            }
        }
    }
    
    private int scan() {
        int cnt = 0;
        for(int i = 0, len = _dataArray.length(); i < len; i++) {
            if(_dataArray.hasData(i)) cnt++;
        }
        return cnt;
    }
    
    public synchronized void rehash() throws Exception {
        if(isOpen()) {
            while(canSplit()) {
                split();
            }
            sync();
        } else {
            throw new StoreClosedException();
        }
    }
    
    /**
     * @return the status of this data set.
     */
    public String getStatus() {
        StringBuilder buf = new StringBuilder();
        
        buf.append("mode=").append(isOpen() ? Mode.OPEN : Mode.CLOSED);
        buf.append(" level=").append(_level);
        buf.append(" split=").append(_split);
        buf.append(" capacity=").append(getCapacity());
        buf.append(" loadCount=").append(_loadCount);
        buf.append(" loadFactor=").append(getLoadFactor());
        
        return buf.toString();
    }
    
    /**
     * @return the home directory of this data set.
     */
    public final File getHomeDir() {
        return _homeDir;
    }
    
    /**
     * @return the underlying data array.
     */
    public final DataArray getDataArray() {
        return _dataArray;
    }
    
    @Override
    public boolean isOpen() {
        return _dataArray.isOpen();
    }
    
    @Override
    public synchronized void open() throws IOException {
        if(!_dataArray.isOpen()) {
            try {
                _dataArray.open();
                _loadCount = scan();
                initLinearHashing();
            } catch (Exception e) {
                try {
                    _dataArray.close();
                } catch(Exception e2) {
                    _log.error("Failed to close", e2);
                }
                
                throw (e instanceof IOException) ? (IOException)e : new IOException(e);
            }
            
            _log.info(getStatus());
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(_dataArray.isOpen()) {
            _dataArray.close();
        }
    }
}
