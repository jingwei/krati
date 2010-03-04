package krati.mds.impl.segment;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.apache.log4j.Logger;

/**
 * SegmentMeta: Meta Data for Segments
 * 
 * <pre>
 *          WORKING-SECTION  COMPACT-SECTION
 *          
 *          SEGGEN  SEGLOAD  SEGGEN  SEGLOAD
 * 00000000    5       ?        6       ?
 * 00000010    0       ?        0       ?
 * 00000020    1       ?        0       ?
 * 00000030    1       ?        1       ?
 * 00000040    1       ?        1       ?
 * 00000050    0       ?        1       ?
 * 00000060    .       .        .       .
 * <pre>
 * 
 * @author jwu
 */
public class SegmentMeta
{
    private final static Logger _log = Logger.getLogger(SegmentMeta.class);
    
    private final static int FREE_SEGMENT = 0;
    private final static int LIVE_SEGMENT = 1;
    
    private final int _segmentDataShift    = 4;
    private final int _segmentDataStart    = 1 << _segmentDataShift;
    private final int _bytesPerSegment     = 1 << _segmentDataShift;
    private final int _bytesPerSection     = _bytesPerSegment >> 1;
    private final int _initialSegmentCount = 100;
    
    private final File _metaFile;
    private RandomAccessFile _raf;
    private FileLock _lock = null;
    private FileChannel _channel = null;
    private MappedByteBuffer _mmapBuffer;
    
    private int _workingGeneration = 0;
    private int _workingSectionOffset = 0; // must be 0 or 8
    
    private int _liveSegmentCount = 0;
    private int _totalSegmentCount = 0;
    
    public SegmentMeta(File file) throws IOException
    {
        this._metaFile = file;
        this.init();
    }
    
    private long getInitialSizeBytes()
    {
        return _segmentDataStart + (_initialSegmentCount * _bytesPerSegment); 
    }
    
    private void createBuffer() throws IOException
    {
        long bufferLength = _raf.length();
        _mmapBuffer = _raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, bufferLength);
    }
    
    private void init() throws IOException
    {
        boolean newFile = false;
        
        if (!_metaFile.exists())
        {
            if (!_metaFile.createNewFile())
            {
                String msg = "Failed to create " + _metaFile.getAbsolutePath();
                
                _log.error(msg);
                throw new IOException(msg);
            }
            
            newFile = true;
        }
        
        _raf = new RandomAccessFile(_metaFile, "rw");
        if (newFile)
        {
            _raf.setLength(getInitialSizeBytes());
            _workingGeneration = 0;
            _workingSectionOffset = 0;
            _liveSegmentCount = 0;
            _totalSegmentCount = _initialSegmentCount;
            
            createBuffer(); // Create MappedByteBuffer
            
            _log.info(_metaFile.getCanonicalPath() + " created");
        }
        else
        {
            createBuffer(); // Create MappedByteBuffer
            
            _workingSectionOffset = 0;
            int gen1 = readInt(_workingSectionOffset);
            int cnt1 = readInt(_workingSectionOffset + 4);
            int gen2 = readInt(_workingSectionOffset + _bytesPerSection);
            int cnt2 = readInt(_workingSectionOffset + _bytesPerSection + 4);
            
            if(Math.abs(gen1 - gen2) == 1)
            {
                // n : (n+1)
                _workingGeneration = Math.max(gen1, gen2);
            }
            else
            {
                // (Integer.MAX_VALUE-1) : 0
                _workingGeneration = Math.min(gen1, gen2);
            }
            if(_workingGeneration == gen2) _workingSectionOffset += _bytesPerSection; 
            _liveSegmentCount = (_workingGeneration == gen1) ? cnt1 : cnt2;
            _totalSegmentCount = (int)(_raf.length() - _segmentDataStart) / _bytesPerSegment;
            
            _log.info(_metaFile.getCanonicalPath() + " loaded");
        }
        _log.info("workingGeneration=" + _workingGeneration + " liveSegmentCount=" + _liveSegmentCount);
    }

    private void writeInt(int pos, int value)
    {
        _mmapBuffer.putInt(pos, value);
    }
    
    private int readInt(int pos)
    {
        return _mmapBuffer.getInt(pos);
    }
    
    public File getMetaFile()
    {
        return _metaFile;
    }
    
    public boolean lock() throws IOException
    {
        try
        {
            _channel = _raf.getChannel();
            _lock = _channel.lock(0, Long.MAX_VALUE, false /* exclusive lock */);
            return true;
        }
        catch(Exception e)
        {
            if(_lock != null) _lock.release();
            if(_channel != null) _channel.close();
        }
        
        return false;
    }
    
    public boolean unlock() throws IOException
    {
       if(_lock != null) _lock.release();
       if(_channel != null) _channel.close();
       return true;
    }
    
    public synchronized int countSegmentsInService()
    {
        return _liveSegmentCount;
    }
    
    public synchronized boolean hasSegmentInService(int segmentId)
    {
        if(segmentId < _totalSegmentCount)
        {
            int pos = _segmentDataStart + (segmentId << _segmentDataShift) + _workingSectionOffset;
            return LIVE_SEGMENT == readInt(pos);
        }
        
        return false;
    }
    
    public synchronized int getSegmentLoadSize(int segmentId)
    {
        if(segmentId < _totalSegmentCount)
        {
            int pos = _segmentDataStart + (segmentId << _segmentDataShift) + _workingSectionOffset;
            return readInt(pos+4);
        }
        
        return 0;
    }
    
    public synchronized int getCapacity()
    {
        return _totalSegmentCount;
    }
    
    public synchronized void ensureCapacity(int segmentCount) throws IOException
    {
        long oldLength = _raf.length();
        long newLength = _segmentDataStart + (segmentCount * _bytesPerSegment);
        if (oldLength < newLength)
        {
            _raf.setLength(newLength);
            createBuffer();
            _totalSegmentCount = segmentCount;
        }
    }
    
    /**
     * Wrap a segment manager and persist its meta data into the .meta file.
     * 
     * @param segmentManager  manager for segments
     * @throws IOException
     */
    public synchronized void wrap(SegmentManager segmentManager) throws IOException
    {
        int newWorkingGeneration = (_workingGeneration + 1) % Integer.MAX_VALUE;
        int newWorkingSectionOffset = (_workingSectionOffset + _bytesPerSection) % _bytesPerSegment;
        int cnt = segmentManager.getSegmentCount();
        int pos = _segmentDataStart + newWorkingSectionOffset;
        
        // Ensure buffer capacity
        ensureCapacity(cnt);
        
        int newLiveSegmentCount = segmentManager.getLiveSegmentCount();
        
        // Update compact-section
        for(int index = 0; index < cnt; index++)
        {
            Segment seg = segmentManager.getSegment(index);
            if(seg != null)
            {
                if(seg.getLoadSize() > Segment.dataStartPosition)
                {
                    // Segment populated with data
                    writeInt(pos, LIVE_SEGMENT);
                    writeInt(pos + 4, seg.getLoadSize());
                }
                else
                {
                    // Segment initialized without any data
                    writeInt(pos, FREE_SEGMENT);
                    writeInt(pos + 4, 0);
                    
                    // Treat it as a non-live segment
                    newLiveSegmentCount--;
                }
            }
            else
            {
                // Segment not alive
                writeInt(pos, FREE_SEGMENT);
                writeInt(pos + 4, 0);
            }
            pos += _bytesPerSegment;
        }
        _mmapBuffer.force();
        
        writeInt(newWorkingSectionOffset, newWorkingGeneration);
        writeInt(newWorkingSectionOffset+4, newLiveSegmentCount);
        _mmapBuffer.force();
        
        // Switch working section to the new one
        _liveSegmentCount = newLiveSegmentCount;
        _workingGeneration = newWorkingGeneration;
        _workingSectionOffset = newWorkingSectionOffset;
        
        _log.info(_metaFile.getCanonicalPath() + " updated");
        _log.info("workingGeneration=" + _workingGeneration + " liveSegmentCount=" + _liveSegmentCount);
    }
}
