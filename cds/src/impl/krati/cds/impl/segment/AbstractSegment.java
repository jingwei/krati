package krati.cds.impl.segment;

import java.io.File;
import java.io.IOException;
import java.util.Date;

/**
 * AbstractSegment
 * 
 * @author jwu
 *
 */
public abstract class AbstractSegment implements Segment
{
    protected final int _segId;
    protected final File _segFile;
    protected final int _initSizeMB;
    protected final long _initSizeBytes;
    protected volatile int _loadSizeBytes;
    protected volatile long _lastForcedTime;
    protected volatile Segment.Mode _segMode;
    
    protected long _storageVersion;
    
    protected AbstractSegment(int segmentId, File segmentFile, int initialSizeMB, Segment.Mode mode) throws IOException
    {
        this._segId = segmentId;
        this._segFile = segmentFile;
        this._initSizeMB = initialSizeMB;
        this._initSizeBytes = initialSizeMB * 1024L * 1024L;
        this._segMode = mode;
        this.init();
    }
    
    protected abstract void init() throws IOException;
    
    protected void initHeader() throws IOException
    {
        // update the time stamp of segment
        _lastForcedTime = System.currentTimeMillis();
        _storageVersion = Segment.STORAGE_VERSION;
        
        setAppendPosition(0);
        appendLong(getLastForcedTime());
        appendLong(getStorageVersion());
        force();
        
        setAppendPosition(Segment.dataStartPosition);
    }
    
    protected void loadHeader() throws IOException
    {
        _lastForcedTime = readLong(posLastForcedTime);
        _storageVersion = readLong(posStorageVersion);
    }
    
    protected String getHeader()
    {
        StringBuffer b = new StringBuffer();
        
        b.append("lastForcedTime");
        b.append('=');
        b.append(new Date(getLastForcedTime()));
        
        b.append(' ');
        
        b.append("storageVersion");
        b.append('=');
        b.append(getStorageVersion());
        
        return b.toString();
    }
    
    @Override
    public Mode getMode()
    {
        return _segMode;
    }
    
    @Override
    public int getSegmentId()
    {
        return _segId;
    }
    
    @Override
    public File getSegmentFile()
    {
        return _segFile;
    }
    
    @Override
    public int getInitialSizeMB()
    {
        return _initSizeMB;
    }
    
    @Override
    public long getInitialSize() {
        return _initSizeBytes;
    }
    
    @Override
    public long getLastForcedTime()
    {
        return _lastForcedTime;
    }
    
    @Override
    public long getStorageVersion()
    {
        return _storageVersion;
    }
    
    @Override
    public double getLoadFactor()
    {
       return ((double)_loadSizeBytes) / _initSizeBytes;
    }

    @Override
    public int getLoadSize()
    {
        return _loadSizeBytes;
    }

    @Override
    public void incrLoadSize(int byteCnt)
    {
        _loadSizeBytes += byteCnt;
    }
    
    @Override
    public void decrLoadSize(int byteCnt)
    {
        _loadSizeBytes -= byteCnt;
    }
}
