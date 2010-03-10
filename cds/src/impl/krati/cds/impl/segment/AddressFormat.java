package krati.cds.impl.segment;

/**
 * AddressFormat
 * 
 * @author jwu
 *
 */
public class AddressFormat
{
    private final int _numSegmentBits;
    private final int _numOffsetBits = 32;
    private final int _segmentMask;
    private final int _offsetMask;
    
    public AddressFormat(int numSegmentBits)
    {
        if(numSegmentBits < 1 || 31 < numSegmentBits)
        {
            throw new IllegalArgumentException("Invalid numSegmentBits: " + numSegmentBits);
        }
        
        _numSegmentBits = numSegmentBits;
        _segmentMask = (1 << _numSegmentBits) - 1;
        _offsetMask = Integer.MAX_VALUE;
    }
    
    public int countOffsetBits()
    {
        return _numOffsetBits;
    }
    
    public int countSegmentBits()
    {
        return _numSegmentBits;
    }
    
    public int getAddressShift()
    {
        return _numSegmentBits + _numOffsetBits;
    }
    
    public int getSegmentShift()
    {
        return _numOffsetBits;
    }
    
    public int getSegmentMask()
    {
        return _segmentMask;
    }
    
    public int getOffsetShift()
    {
        return 0;
    }
    
    public int getOffsetMask()
    {
        return _offsetMask;
    }
}
