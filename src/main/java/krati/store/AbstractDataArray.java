package krati.store;

import java.io.File;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import krati.Persistable;
import krati.array.DataArray;
import krati.core.array.AddressArray;
import krati.core.array.SimpleDataArray;
import krati.core.segment.SegmentFactory;
import krati.core.segment.SegmentManager;

/**
 * AbstractDataArray
 * 
 * @author jwu
 * 09/24, 2010
 */
public abstract class AbstractDataArray implements DataArray, Persistable {
    protected final SimpleDataArray _dataArray;
    protected final AddressArray _addrArray;
    protected final String _homePath;
    protected final File _homeDir;
    
    /**
     * Constructs a data array.
     * 
     * @param length               - the array length
     * @param batchSize            - the number of updates per update batch
     * @param numSyncBatches       - the number of update batches required for updating the underlying address array
     * @param homeDirectory        - the home directory of data array
     * @param segmentFactory       - the segment factory
     * @param segmentFileSizeMB    - the segment size in MB
     * @param segmentCompactFactor - the segment load threshold, below which a segment is eligible for compaction
     * @throws Exception
     */
    protected AbstractDataArray(int length,
                                int batchSize,
                                int numSyncBatches,
                                File homeDirectory,
                                SegmentFactory segmentFactory,
                                int segmentFileSizeMB,
                                double segmentCompactFactor) throws Exception {
        this._homeDir = homeDirectory;
        this._homePath = homeDirectory.getCanonicalPath();
        
        // Create address array 
        _addrArray = createAddressArray(length, batchSize, numSyncBatches, homeDirectory);
        
        // Create segment manager
        String segmentHome = this._homePath + File.separator + "segs";
        SegmentManager segmentManager = SegmentManager.getInstance(segmentHome,
                                                               segmentFactory,
                                                               segmentFileSizeMB);
        
        // Create data array
        this._dataArray = new SimpleDataArray(_addrArray, segmentManager, segmentCompactFactor);
    }
    
    protected abstract AddressArray createAddressArray(int length,
                                                       int batchSize,
                                                       int numSyncBatches,
                                                       File homeDirectory) throws Exception;
    
    public File getHomeDir() {
        return _homeDir;
    }
    
    public String getHomePath() {
        return _homePath;
    }
    
    public String getStatus() {
        StringBuilder buffer = new StringBuilder();
        
        buffer.append("path");
        buffer.append("=");
        buffer.append(_homePath);
        buffer.append(" ");
        
        buffer.append("length");
        buffer.append("=");
        buffer.append(length());
        buffer.append(" ");
        
        buffer.append("lwMark");
        buffer.append("=");
        buffer.append(getLWMark());
        buffer.append(" ");

        buffer.append("hwMark");
        buffer.append("=");
        buffer.append(getHWMark());
        
        return buffer.toString();
    }

    @Override
    public int length() {
        return _dataArray.length();
    }

    @Override
    public boolean hasIndex(int index) {
        return _dataArray.hasIndex(index);
    }
    
    @Override
    public byte[] get(int index) {
        return _dataArray.get(index);
    }
    
    @Override
    public int get(int index, byte[] dst) {
        return _dataArray.get(index, dst);
    }
    
    @Override
    public int get(int index, byte[] dst, int offset) {
        return _dataArray.get(index, dst, offset);
    }
    
    @Override
    public int getLength(int index) {
        return _dataArray.getLength(index);
    }
    
    @Override
    public boolean hasData(int index) {
        return _dataArray.hasData(index);
    }
    
    @Override
    public synchronized void set(int index, byte[] data, long scn) throws Exception {
        _dataArray.set(index, data, scn);
    }
    
    @Override
    public synchronized void set(int index, byte[] data, int offset, int length, long scn) throws Exception {
        _dataArray.set(index, data, offset, length, scn);
    }
    
    @Override
    public int transferTo(int index, WritableByteChannel channel) {
        return _dataArray.transferTo(index, channel);
    }
    
    @Override
    public synchronized void clear() {
        _dataArray.clear();
    }
    
    @Override
    public long getLWMark() {
        return _dataArray.getLWMark();
    }
    
    @Override
    public long getHWMark() {
        return _dataArray.getHWMark();
    }
    
    @Override
    public synchronized void saveHWMark(long endOfPeriod) throws Exception {
        _dataArray.saveHWMark(endOfPeriod);
    }
    
    @Override
    public synchronized void persist() throws IOException {
        _dataArray.persist();
    }
    
    @Override
    public synchronized void sync() throws IOException {
        _dataArray.sync();
    }
}
