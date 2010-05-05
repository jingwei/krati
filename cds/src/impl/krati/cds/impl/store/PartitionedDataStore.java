package krati.cds.impl.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import krati.cds.DataCache;
import krati.cds.impl.DataCacheImpl;
import krati.cds.impl.segment.SegmentFactory;
import krati.cds.store.DataStore;
import krati.util.FnvHashFunction;
import krati.util.HashFunction;

public class PartitionedDataStore implements DataStore<byte[], byte[]>
{
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
                                int partitionCount, int partitionCapacity) throws Exception
    {
        this._partitionHome= partitionHome;
        this._partitionCount = partitionCount;
        this._partitionCapacity = partitionCapacity;
        this._totalCapacity = partitionCount * partitionCapacity;
        this._hashFunction = new FnvHashFunction();
        this.init(new krati.cds.impl.segment.MemorySegmentFactory(), 256);
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
                                SegmentFactory segFactory, int segFileSizeMB) throws Exception
    {
        this._partitionHome= partitionHome;
        this._partitionCount = partitionCount;
        this._partitionCapacity = partitionCapacity;
        this._totalCapacity = partitionCount * partitionCapacity;
        this._hashFunction = new FnvHashFunction();
        this.init(segFactory, segFileSizeMB);
    }
    
    protected void init(SegmentFactory segFactory, int segFileSizeMB) throws Exception
    {
        _log.info("partitionHome=" + _partitionHome.getCanonicalPath() +
                  " partitionCount=" + _partitionCount +
                  " partitionCapacity=" + _partitionCapacity);
        
        _partitionList = new ArrayList<DataStore<byte[], byte[]>>(_partitionCount);
        for(int i = 0; i < _partitionCount; i++)
        {
            DataCache cache = new DataCacheImpl(0,
                                                _partitionCapacity,
                                                new File(_partitionHome, "P" + i),
                                                segFactory, segFileSizeMB);
            _partitionList.add(new SimpleDataStore(cache, _hashFunction));
        }
        
        _log.info("init done");
    }
    
    public File getPartitionHome()
    {
        return _partitionHome;
    }
    
    public int getPartitionCount()
    {
        return _partitionCount;
    }
    
    public int getPartitionCapacity()
    {
        return _partitionCapacity;
    }
    
    public long getTotalCapacity()
    {
        return _totalCapacity;
    }
    
    @Override
    public long hash(byte[] key)
    {
        return _hashFunction.hash(key);
    }
    
    @Override
    public byte[] get(byte[] key)
    {
        long hashCode = hash(key);
        long index = hashCode % _totalCapacity;
        int storeId = (int)(index / _partitionCapacity);
        return _partitionList.get(storeId).get(key);
    }
    
    @Override
    public boolean put(byte[] key, byte[] value) throws Exception
    {
        long hashCode = hash(key);
        long index = hashCode % _totalCapacity;
        int storeId = (int)(index / _partitionCapacity);
        return _partitionList.get(storeId).put(key, value);
    }

    @Override
    public boolean delete(byte[] key) throws Exception
    {
        long hashCode = hash(key);
        long index = hashCode % _totalCapacity;
        int storeId = (int)(index / _partitionCapacity);
        return _partitionList.get(storeId).delete(key);
    }
    
    @Override
    public void persist() throws IOException
    {
        for(DataStore<byte[], byte[]> storeImpl: _partitionList)
        {
            storeImpl.persist();
        }
        
        _log.info("store persisted");
    }
    
    @Override
    public void clear() throws IOException
    {
        for(DataStore<byte[], byte[]> storeImpl: _partitionList)
        {
            storeImpl.clear();
        }
    }
}
