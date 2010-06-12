package krati.cds.impl.store;

import java.nio.ByteBuffer;

/**
 * Data Store Utilities.
 * 
 * @author jwu
 *
 */
final class DataStoreUtils
{
    static byte[] assemble(byte[] key, byte[] value)
    {
        byte[] result = new byte[4 + 4 + key.length + 4 + value.length];
        ByteBuffer bb = ByteBuffer.wrap(result);
        
        // count
        bb.putInt(1);
        
        // add key
        bb.putInt(key.length);
        bb.put(key);
        
        // add value
        bb.putInt(value.length);
        bb.put(value);
        
        return result;
    }
    
    static byte[] assemble(byte[] existingData, byte[] key, byte[] value)
    {
        // Remove old data
        int newLength = removeByKey(key, existingData);
        if(newLength == 0) return assemble(key, value);
        
        byte[] result = new byte[newLength + 4 + key.length + 4 + value.length];
        System.arraycopy(existingData, 0, result, 0, newLength);
        
        ByteBuffer bb = ByteBuffer.wrap(result);
        
        int cnt = bb.getInt();
        
        // update count
        bb.position(0);
        bb.putInt(cnt + 1);
        
        // add key
        bb.position(newLength);
        bb.putInt(key.length);
        bb.put(key);
        
        // add value
        bb.putInt(value.length);
        bb.put(value);
        
        return result;
    }
    
    static byte[] extractByKey(byte[] key, byte[] data)
    {
        if(data.length == 0) return null;
        ByteBuffer bb = ByteBuffer.wrap(data);
        
        int cnt = bb.getInt();
        while(cnt > 0)
        {
            // Process key
            int len = bb.getInt();
            if(keysEqual(key, data, bb.position(), len))
            {
                // pass key data
                bb.position(bb.position() + len);
                
                // Process value
                len = bb.getInt();
                byte[] result = new byte[len];
                bb.get(result);
                
                return result;
            }
            bb.position(bb.position() + len);
            
            // Process value
            len = bb.getInt();
            bb.position(bb.position() + len);
            
            cnt--;
        }
        
        // no data is found for the key
        return null;
    }
    
    static int removeByKey(byte[] key, byte[] data)
    {
        int offset1 = 0;
        int offset2 = 0;
        ByteBuffer bb = ByteBuffer.wrap(data);
        
        int originalCnt = bb.getInt();
        int cnt = originalCnt;
        while(cnt > 0)
        {
            offset1 = bb.position();
            
            // Process key
            int len = bb.getInt();
            if(keysEqual(key, data, bb.position(), len))
            {
                bb.position(bb.position() + len);
                
                // Process value
                len = bb.getInt();
                bb.position(bb.position() + len);
                
                offset2 = bb.position();
                break;
            }
            bb.position(bb.position() + len);
            
            // Process value
            len = bb.getInt();
            bb.position(bb.position() + len);
            
            cnt--;
        }
        
        // key is found and remove key-value from data
        if(offset1 < offset2)
        {
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
            for(int i = 0, len = data.length - offset2; i < len; i++)
            {
                data[offset1 + i] = data[offset2 + i];
            }
            
            return newLength;
        }
        
        // no data is removed
        return data.length;
    }
    
    static boolean keysEqual(byte[] key, byte[] keySource, int offset, int length)
    {
        if(key.length == length)
        {
            for(int i = 0; i < length; i++)
            {
                if(key[i] != keySource[offset + i]) return false;
            }
            return true;
        }
        
        return false;
    }
}
