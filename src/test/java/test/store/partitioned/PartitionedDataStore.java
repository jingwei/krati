package test.store.partitioned;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import krati.core.segment.SegmentFactory;
import krati.store.DataStore;
import krati.store.StaticDataStore;
import krati.util.FnvHashFunction;
import krati.util.HashFunction;

/**
 * PartitionedDataStore.
 * 
 * @author jwu
 *
 */
public class PartitionedDataStore implements DataStore<byte[], byte[]> {
    private final static Logger _log = Logger.getLogger(PartitionedDataStore.class);
    
    private final File _partitionHome;
    private final int  _partitionCount;
    private final int  _partitionCapacity;
    private final long _totalCapacity;
    private final HashFunction<byte[]> _hashFunction;
    private List<DataStore<byte[], byte[]>> _partitionList;
    
    /**
     * Creates a new <code>PartitionedDataStore</code> with the following default configurations.
     * 
     * <pre>
     *   Segment Factory:        MemorySegmentFactory
     *   Segment File Size:      256MB
     * </pre>
     * 
     * @param partitionHome      partition home directory.
     * @param partitionCount     the count of all partitions.
     * @param partitionCapacity  the capacity of each partition.
     * @throws Exception         if the store cannot be initiated.
     */
    public PartitionedDataStore(File partitionHome,
                                int partitionCount, int partitionCapacity) throws Exception {
        this._partitionHome= partitionHome;
        this._partitionCount = partitionCount;
        this._partitionCapacity = partitionCapacity;
        this._totalCapacity = partitionCount * partitionCapacity;
        this._hashFunction = new FnvHashFunction();
        this.init(new krati.core.segment.MemorySegmentFactory(), 256);
    }
    
    /**
     * Creates a new <code>PartitionedDataStore</code>.
     * 
     * @param partitionHome      partition home directory.
     * @param partitionCount     the count of all partitions.
     * @param partitionCapacity  the capacity of each partition.
     * @param segFactory         the segment factory.
     * @param segFileSizeMB      the size of segment.
     * @throws Exception         if the store cannot be initiated.
     */
    public PartitionedDataStore(File partitionHome,
                                int partitionCount, int partitionCapacity,
                                SegmentFactory segFactory, int segFileSizeMB) throws Exception {
        this._partitionHome= partitionHome;
        this._partitionCount = partitionCount;
        this._partitionCapacity = partitionCapacity;
        this._totalCapacity = partitionCount * partitionCapacity;
        this._hashFunction = new FnvHashFunction();
        this.init(segFactory, segFileSizeMB);
    }
    
    protected void init(SegmentFactory segFactory, int segFileSizeMB) throws Exception {
        _log.info("partitionHome=" + _partitionHome.getCanonicalPath() +
                  " partitionCount=" + _partitionCount +
                  " partitionCapacity=" + _partitionCapacity +
                  " segmentFactory=" + segFactory.getClass().getName() +
                  " segmentFileSizeMB=" + segFileSizeMB + "MB");
        
        _partitionList = new ArrayList<DataStore<byte[], byte[]>>(_partitionCount);
        for (int i = 0; i < _partitionCount; i++) {
            StaticDataStore subStore =
                new StaticDataStore(
                    new File(_partitionHome, "P" + i),
                    _partitionCapacity,
                    10000,
                    5,
                    segFileSizeMB,
                    segFactory);
            _partitionList.add(subStore);
        }
        
        _log.info("init done");
    }
    
    public File getPartitionHome() {
        return _partitionHome;
    }
    
    public int getPartitionCount() {
        return _partitionCount;
    }
    
    public int getPartitionCapacity() {
        return _partitionCapacity;
    }
    
    public long getTotalCapacity() {
        return _totalCapacity;
    }
    
    protected long hash(byte[] key) {
        return _hashFunction.hash(key);
    }
    
    @Override
    public byte[] get(byte[] key) {
        long hashCode = hash(key);
        long index = hashCode % _totalCapacity;
        if (index < 0) index = -index;
        
        int storeId = (int)(index / _partitionCapacity);
        return _partitionList.get(storeId).get(key);
    }
    
    @Override
    public boolean put(byte[] key, byte[] value) throws Exception {
        long hashCode = hash(key);
        long index = hashCode % _totalCapacity;
        if (index < 0) index = -index;
        
        int storeId = (int)(index / _partitionCapacity);
        return _partitionList.get(storeId).put(key, value);
    }

    @Override
    public boolean delete(byte[] key) throws Exception {
        long hashCode = hash(key);
        long index = hashCode % _totalCapacity;
        if (index < 0) index = -index;
        
        int storeId = (int)(index / _partitionCapacity);
        return _partitionList.get(storeId).delete(key);
    }
    
    @Override
    public void sync() throws IOException {
        for (DataStore<byte[], byte[]> storeImpl : _partitionList) {
            storeImpl.sync();
        }
        
        _log.info("store saved");
    }
    
    @Override
    public void persist() throws IOException {
        for (DataStore<byte[], byte[]> storeImpl : _partitionList) {
            storeImpl.persist();
        }
        
        _log.info("store persisted");
    }
    
    @Override
    public void clear() throws IOException {
        for (DataStore<byte[], byte[]> storeImpl : _partitionList) {
            storeImpl.clear();
        }
    }
    
    @Override
    public Iterator<byte[]> keyIterator() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Iterator<Entry<byte[], byte[]>> iterator() {
        throw new UnsupportedOperationException();
    }
}
