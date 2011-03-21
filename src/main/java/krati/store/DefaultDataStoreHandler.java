package krati.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

import org.apache.log4j.Logger;

import krati.store.DataStoreHandler;

/**
 * DefaultDataStoreHandler
 * 
 * @author jwu
 *
 */
public final class DefaultDataStoreHandler implements DataStoreHandler {
    private final static Logger _log = Logger.getLogger(DefaultDataStoreHandler.class);
    private final static int NUM_BYTES_IN_INT = 4;
    
    @Override
    public final byte[] assemble(byte[] key, byte[] value) {
        if(value == null) return null;
        
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
    
    @Override
    public final byte[] assemble(byte[] key, byte[] value, byte[] data) {
        if(data == null || data.length == 0) {
            return assemble(key, value); 
        }
        
        // Remove old data
        int newLength = removeByKey(key, data);
        if(newLength == 0) return assemble(key, value);
        if(value == null) return Arrays.copyOf(data, newLength);
        
        byte[] result = new byte[newLength + 4 + key.length + 4 + value.length];
        System.arraycopy(data, 0, result, 0, newLength);
        
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
    
    @Override
    public final int countCollisions(byte[] key, byte[] data) {
        if(data == null || data.length == 0) {
            return 0;
        }
        
        try {
            ByteBuffer bb = ByteBuffer.wrap(data);
            int originalCnt = bb.getInt();
            int cnt = originalCnt;
            while(cnt > 0) {
                // Process key
                int len = bb.getInt();
                if(keysEqual(key, data, bb.position(), len)) {
                    return originalCnt;
                }
                bb.position(bb.position() + len);
                
                // Process value
                len = bb.getInt();
                bb.position(bb.position() + len);
                
                cnt--;
            }
            
            return -originalCnt;
        } catch (Exception e) {
            _log.error("Failed to countCollisions", e);
            return 0;
        }
    }
    
    @Override
    public final byte[] extractByKey(byte[] key, byte[] data) {
        if(data.length == 0) return null;
        ByteBuffer bb = ByteBuffer.wrap(data);
        
        int cnt = bb.getInt();
        while(cnt > 0) {
            // Process key
            int len = bb.getInt();
            if(keysEqual(key, data, bb.position(), len)) {
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
    
    @Override
    public final int removeByKey(byte[] key, byte[] data) {
        int offset1 = 0;
        int offset2 = 0;
        ByteBuffer bb = ByteBuffer.wrap(data);
        
        int originalCnt = bb.getInt();
        int cnt = originalCnt;
        while(cnt > 0) {
            offset1 = bb.position();
            
            // Process key
            int len = bb.getInt();
            if(keysEqual(key, data, bb.position(), len)) {
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
    
    static boolean keysEqual(byte[] key, byte[] keySource, int offset, int length) {
        if (key.length == length) {
            for (int i = 0; i < length; i++) {
                if (key[i] != keySource[offset + i])
                    return false;
            }
            return true;
        }
        
        return false;
    }

    @Override
    public final List<byte[]> extractKeys(byte[] data) {
        try {
            ByteBuffer bb = ByteBuffer.wrap(data);
            int cnt = bb.getInt();
            final List<byte[]> result = new ArrayList<byte[]>(cnt);
            
            while(cnt > 0) {
                // Process key
                int len = bb.getInt();
                byte[] key = new byte[len];
                bb.get(key);
                
                // Add to result
                result.add(key);
                
                // Process value
                len = bb.getInt();
                bb.position(bb.position() + len);
                
                cnt--;
            }
            
            return result;
        } catch(Exception e) {
            _log.error("Failed to extractKeys", e);
            return null;
        }
    }
    
    public final List<byte[]> extractValues(byte[] data) {
        try {
            ByteBuffer bb = ByteBuffer.wrap(data);
            int cnt = bb.getInt();
            final List<byte[]> result = new ArrayList<byte[]>(cnt);
            
            while(cnt > 0) {
                // Process key
                int len = bb.getInt();
                bb.position(bb.position() + len);
                
                // Process value
                len = bb.getInt();
                byte[] value = new byte[len];
                bb.get(value);
                
                // Add to result
                result.add(value);
                
                cnt--;
            }
            
            return result;
        } catch (Exception e) {
            _log.error("Failed to extractValues", e);
            return null;
        }
    }
    
    @Override
    public final List<Entry<byte[], byte[]>> extractEntries(byte[] data) {
        try {
            ByteBuffer bb = ByteBuffer.wrap(data);
            int cnt = bb.getInt();
            final List<Entry<byte[], byte[]>> result = new ArrayList<Entry<byte[], byte[]>>(cnt);
            
            while(cnt > 0) {
                // Process key
                int len = bb.getInt();
                byte[] key = new byte[len];
                bb.get(key);
                
                // Process value
                len = bb.getInt();
                byte[] val = new byte[len];
                bb.get(val);
                
                // Add to result
                result.add(new SimpleEntry<byte[], byte[]>(key, val));
                
                cnt--;
            }
            
            return result;
        } catch(Exception e) {
            _log.error("Failed to extractEntries", e);
            return null;
        }
    }
    
    @Override
    public final byte[] assembleEntries(List<Entry<byte[], byte[]>> entries) {
        byte[] b;
        int cnt = 0;
        int len = NUM_BYTES_IN_INT;
        
        for(Entry<byte[], byte[]> e : entries) {
            b = e.getKey();
            if(b != null) {
                len += NUM_BYTES_IN_INT;
                len += b.length;
                
                b = e.getValue();
                len += NUM_BYTES_IN_INT;
                len += b == null ? 0 : e.getValue().length;
                
                cnt++;
            }
        }
        
        byte[] data = new byte[len];
        ByteBuffer bb = ByteBuffer.wrap(data);
        
        bb.putInt(cnt);
        for(Entry<byte[], byte[]> e : entries) {
            b = e.getKey();
            if(b != null) {
                bb.putInt(b.length);
                bb.put(b);
                
                b = e.getValue();
                if(b == null) {
                    bb.putInt(0);
                } else {
                    bb.putInt(b.length);
                    bb.put(b);
                }
            }
        }
        
        return data;
    }
}
