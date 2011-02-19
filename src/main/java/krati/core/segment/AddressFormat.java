package krati.core.segment;

/**
 * AddressFormat
 * 
 * @author jwu
 *
 */
public final class AddressFormat {
    private final int _numDataSizeBits;
    private final int _numSegmentBits;
    private final int _numOffsetBits;
    private final int _dataSizeShift;
    private final int _dataSizeMask;
    private final int _segmentMask;
    private final int _offsetMask;
    private final int _maxDataSize;
    
    public AddressFormat() {
        this(32, 16);
    }
    
    protected AddressFormat(int numOffsetBits, int numSegmentBits) {
        if(numOffsetBits < 1 || 32 < numOffsetBits) {
            throw new IllegalArgumentException("Invalid numOffsetBits: " + numOffsetBits);
        }
        
        if(numSegmentBits < 1 || 31 < numSegmentBits) {
            throw new IllegalArgumentException("Invalid numSegmentBits: " + numSegmentBits);
        }
        
        _numOffsetBits = numOffsetBits;
        _numSegmentBits = numSegmentBits;
        _numDataSizeBits = 64 - _numSegmentBits - _numOffsetBits;
        
        _offsetMask = (numOffsetBits >= 31) ? Integer.MAX_VALUE : ((1 << _numOffsetBits) - 1);
        _segmentMask = (1 << _numSegmentBits) - 1;
        _dataSizeMask = (1 << _numDataSizeBits) - 1;
        _maxDataSize = _dataSizeMask;
        _dataSizeShift = _numSegmentBits + _numOffsetBits;
    }
    
    public final int countOffsetBits() {
        return _numOffsetBits;
    }
    
    public final int countSegmentBits() {
        return _numSegmentBits;
    }
    
    public final int countDataSizeBits() {
        return _numDataSizeBits;
    }
    
    public final int getDataSizeShift() {
        return _dataSizeShift;
    }
    
    public final int getDataSizeMask() {
        return _dataSizeMask;
    }
    
    public final int getSegmentShift() {
        return _numOffsetBits;
    }
    
    public final int getSegmentMask() {
        return _segmentMask;
    }
    
    public final int getOffsetShift() {
        return 0;
    }
    
    public final int getOffsetMask() {
        return _offsetMask;
    }
    
    public final int getOffset(long addr) {
        return (int)(addr & _offsetMask);
    }
    
    public final int getSegment(long addr) {
        return ((int)(addr >> _numOffsetBits) & _segmentMask);
    }
    
    public final int getDataSize(long addr) {
        return ((int)(addr >> _dataSizeShift) & _dataSizeMask);
    }
    
    public final int getMaxDataSize() {
        return _maxDataSize;
    }
    
    public final long composeAddress(int offset, int segment, int dataSize) {
        if(dataSize > _maxDataSize) {
            return (offset | ((long)segment << _numOffsetBits));
        } else {
            return (offset | ((long)segment << _numOffsetBits) | ((long)dataSize << _dataSizeShift));
        }
    }
    
    public final String toString(long address) {
        long offset = getOffset(address);
        long segment = getSegment(address);
        long dataSize = getDataSize(address);
        
        return String.format("Address=%d [offset=%d segment=%d size=%d]", address, offset, segment, dataSize);
    }
}
