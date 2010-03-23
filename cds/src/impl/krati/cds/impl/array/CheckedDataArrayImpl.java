package krati.cds.impl.array;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import krati.cds.impl.array.fixed.LongArrayRecoverableImpl;
import krati.cds.impl.segment.Segment;
import krati.cds.impl.segment.SegmentManager;

public class CheckedDataArrayImpl extends DataArrayImpl
{
    private final int _checksumBits = 11;
    private final int _checksumBatchLength = 1 << _checksumBits;
    private byte[] _checkedData = new byte[_checksumBatchLength + 8];
    private ByteBuffer _checkedBuffer = ByteBuffer.wrap(_checkedData);
    

    public CheckedDataArrayImpl(LongArrayRecoverableImpl addressArray,
                                SegmentManager segmentManager)
    {
        this(addressArray, segmentManager, 0.2, 0.5);
    }
    
    public CheckedDataArrayImpl(LongArrayRecoverableImpl addressArray,
                                SegmentManager segmentManager,
                                double segmentCompactTrigger,
                                double segmentCompactFactor)
    {
        super(addressArray,
              segmentManager,
              segmentCompactTrigger,
              segmentCompactFactor);
    }

    @Override
    public byte[] getData(int index)
    {
        try
        {
            long address = getAddress(index);
            int segPos = (int)(address & _offsetMask);
            int segInd = (int)((address >> _segmentShift) & _segmentMask);
            
            // no data found
            if(segPos < Segment.dataStartPosition) return null;
            
            // get data segment
            Segment seg = _segmentManager.getSegment(segInd);
            
            // read data length
            int len = seg.readInt(segPos);
            
            if(len > 0)
            {
                int remainder = len % (_checksumBatchLength + 8);
                int dataLength = (len / (_checksumBatchLength + 8)) * _checksumBatchLength + ((remainder == 0) ? 0 : (remainder - 8));
                if(dataLength <= 8)
                {
                    // corrupted data
                    return null;
                }
                else
                {
                    byte[] data = new byte[dataLength];
                    getCheckedData(seg, segPos + 4, len, data, 0);
                    return data;
                }
            }
            else if(len == 0)
            {
                return new byte[0];
            }
            else
            {
                return null;
            }
        }
        catch(Exception e)
        {
            return null;
        }
    }
    
    @Override
    public int getData(int index, byte[] data, int offset)
    {
        try
        {
            long address = getAddress(index);
            int segPos = (int)(address & _offsetMask);
            int segInd = (int)((address >> _segmentShift) & _segmentMask);
            
            // no data found
            if(segPos < Segment.dataStartPosition) return -1;
            
            // get data segment
            Segment seg = _segmentManager.getSegment(segInd);
            
            // read data length
            int len = seg.readInt(segPos);

            // read data into byte array
            if (len > 0)
            {
                int dataLength = getCheckedData(seg, segPos + 4, len, data, offset);
                return dataLength;
            }
            else
            {
                return len;
            }
        }
        catch(Exception e)
        {
            return -1;
        }
    }
    
    private int getCheckedData(Segment seg, int readPos, int readLen, byte[] dst, int offset) throws IOException
    {
        int nextLength;
        int nextOffset = offset;
        Checksum checksum = new Adler32();
        
        while(readLen > 0)
        {
            nextLength = (readLen <= _checksumBatchLength) ? (readLen - 8) : _checksumBatchLength;
            
            // read partial data
            seg.read(readPos, dst, nextOffset, nextLength);
            readPos += nextLength;
            
            // calculate partial data checksum
            checksum.update(dst, nextOffset, nextLength);
            long newChksum = checksum.getValue();
            
            // read partial data checksum stored in segment
            long oldChksum = seg.readLong(readPos);
            readPos += 8;
            
            // compare checksum
            if(newChksum != oldChksum)
            {
                return -1;
            }
            
            // advance offset
            nextOffset += nextLength;
            readLen -= (nextLength + 8);
        }
        
        // return the true length of data without counting checksum bytes
        return nextOffset - offset;
    }
    
    @Override
    public void setData(int index, byte[] data, int offset, int length, long scn) throws Exception
    {
        int checkedLength = populateCheckedData(data, offset, length);
        super.setData(index, _checkedData, 0, checkedLength, scn);
    }
    
    private int populateCheckedData(byte[] data, int offset, int length)
    {
        Checksum checksum = new Adler32();
        int chkBatchCnt = (length >> _checksumBits) + 1;
        int checkedLength = (chkBatchCnt << _checksumBits) + (chkBatchCnt << 3);
        if(_checkedData.length < checkedLength)
        {
            _checkedData = new byte[checkedLength];
            _checkedBuffer = ByteBuffer.wrap(_checkedData);
        }
        
        _checkedBuffer.clear();
        int nextOffset = offset;
        int nextLength;
        
        while(length > 0)
        {
            nextLength = (length < _checksumBatchLength) ? length : _checksumBatchLength;
            
            // calculate checksum
            checksum.update(data, nextOffset, nextLength);
            long chksum = checksum.getValue();
            
            // write partial data
            _checkedBuffer.put(data, nextOffset, nextLength);
            nextOffset += nextLength;
            
            // write partial data checksum
            _checkedBuffer.putLong(chksum);
            nextOffset += 8;
            
            length -= nextLength;
        }
        
        return (nextOffset - offset);
    }
}
