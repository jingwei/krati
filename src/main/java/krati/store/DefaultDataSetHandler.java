package krati.store;

import java.nio.ByteBuffer;

import krati.store.DataSetHandler;

/**
 * DefaultDataSetHandler
 * 
 * @author jwu
 *
 */
public final class DefaultDataSetHandler implements DataSetHandler {
    
    @Override
    public final byte[] assemble(byte[] value) {
        byte[] result = new byte[4 + 4 + value.length];
        ByteBuffer bb = ByteBuffer.wrap(result);
        
        // count
        bb.putInt(1);
        
        // add value
        bb.putInt(value.length);
        bb.put(value);
        
        return result;
    }
    
    @Override
    public final byte[] assemble(byte[] value, byte[] data) {
        byte[] result = new byte[data.length + 4 + value.length];
        System.arraycopy(data, 0, result, 0, data.length);
        ByteBuffer bb = ByteBuffer.wrap(result);
        int cnt = bb.getInt();
        
        // update count
        bb.position(0);
        bb.putInt(cnt + 1);
        
        // add value
        bb.position(data.length);
        bb.putInt(value.length);
        bb.put(value);
        
        return result;
    }
    
    @Override
    public final int count(byte[] data) {
        if(data.length >= 4) {
            ByteBuffer bb = ByteBuffer.wrap(data, 0, 4);
            return bb.getInt();
        }
        
        return 0;
    }
    
    @Override
    public final int countCollisions(byte[] value, byte[] data) {
        try {
            ByteBuffer bb = ByteBuffer.wrap(data);
            int originalCnt = bb.getInt();
            int cnt = originalCnt;
            while(cnt > 0) {
                // Process data value
                int len = bb.getInt();
                if(bytesEqual(value, data, bb.position(), len)) {
                    return originalCnt;
                }
                bb.position(bb.position() + len);
                cnt--;
            }
            
            return -originalCnt;
        } catch(Exception e) {
            return 0;
        }
    }
    
    @Override
    public final int remove(byte[] value, byte[] data) {
        int offset1 = 0;
        int offset2 = 0;
        ByteBuffer bb = ByteBuffer.wrap(data);
        
        int originalCnt = bb.getInt();
        int cnt = originalCnt;
        while(cnt > 0) {
            offset1 = bb.position();
            
            // Process data value
            int len = bb.getInt();
            if(bytesEqual(value, data, bb.position(), len)) {
                offset2 = bb.position() + len;
                break;
            }
            bb.position(bb.position() + len);
            
            cnt--;
        }
        
        // value is found and remove value from data
        if(offset1 < offset2) {
            int newLength = data.length - (offset2 - offset1);
            
            /*
             * entire data is removed
             */
            if(newLength <= 4) return 0;
            
            /*
             * partial data is removed
             */
            
            // update data count
            bb.position(0);
            bb.putInt(originalCnt - 1);
            
            // Shift data to the left
            for(int i = 0, len = data.length - offset2; i < len; i++) {
                data[offset1 + i] = data[offset2 + i];
            }
            
            return newLength;
        }
        
        // no data is removed
        return data.length;
    }
    
    @Override
    public final boolean find(byte[] value, byte[] data) {
        try {
            ByteBuffer bb = ByteBuffer.wrap(data);
            int cnt = bb.getInt();
            while(cnt > 0) {
                // Process data value
                int len = bb.getInt();
                if(bytesEqual(value, data, bb.position(), len)) {
                    return true;
                }
                bb.position(bb.position() + len);
                cnt--;
            }
            
            return false;
        } catch(Exception e) {
            return false;
        }
    }
    
    static boolean bytesEqual(byte[] bytes, byte[] bytesSource, int offset, int length) {
        if (bytes.length == length) {
            for (int i = 0; i < length; i++) {
                if (bytes[i] != bytesSource[offset + i])
                    return false;
            }
            return true;
        }

        return false;
    }
}
