package krati.cds.impl.array.basic;

import java.io.IOException;

import krati.cds.array.LongArray;

public class LongArrayMemoryImpl extends AbstractArray<long[]> implements LongArray
{
    protected long _lwmScn = 0; // Low water mark SCN starts from 0
    protected long _hwmScn = 0; // High water mark SCN starts from 0
    
    public LongArrayMemoryImpl(int memberIdStart, int memberIdCount)
    {
        super(memberIdStart, memberIdCount);
    }

    @Override
    protected void init() 
    { 
        _parallelData = new long[_memberIdCount];
    }
    
    @Override
    public long getData(int index)
    {
      return _parallelData[index - _memberIdStart];
    }
    
    @Override
    public void setData(int index, long value, long scn) throws Exception
    {
      _parallelData[index - _memberIdStart] = value;
      _hwmScn = Math.max(_hwmScn, scn);
    }
    
    @Override
    public void clear()
    {
        if (_parallelData != null)
        {
          for (int i = 0; i < _parallelData.length; i ++)
          {
            _parallelData[i] = 0;
          }
        }
    }
    
    @Override
    public int getIndexStart()
    {
        return _memberIdStart;
    }

    @Override
    public boolean indexInRange(int index)
    {
        return (_memberIdStart <= index && index < _memberIdEnd);
    }

    @Override
    public int length()
    {
        return _memberIdCount;
    }

    @Override
    public long getHWMark()
    {
        return _hwmScn;
    }

    @Override
    public long getLWMark()
    {
        return _lwmScn;
    }

    @Override
    public void sync() throws IOException
    {
        // not supported for memory-based LongArray implementation
    }
    
    @Override
    public void persist() throws IOException
    {
        // not supported for memory-based LongArray implementation
    }
    
    @Override
    public void saveHWMark(long endOfPeriod) throws Exception
    {
        _hwmScn = endOfPeriod;
        _lwmScn = endOfPeriod;
    }
    
    @Override
    public Object memoryClone()
    {
        LongArrayMemoryImpl memClone = new LongArrayMemoryImpl(getIndexStart(), length());
        
        System.arraycopy(_parallelData, 0, memClone.getInternalArray(), 0, _parallelData.length);
        memClone._lwmScn = getLWMark(); 
        memClone._hwmScn = getHWMark();
        
        return memClone;
    }
}