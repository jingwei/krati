/*
 * Copyright (c) 2010-2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package krati.store.handler;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import krati.store.DataStoreHandler;
import krati.util.Bytes;

import org.apache.log4j.Logger;

/**
 * FKFVDataStoreHandler - The fixed-length key and fixed-length value {@link krati.store.DataStoreHandler DataStoreHandler}.
 * 
 * @author jwu
 * @since 08/18, 2012
 */
public class FKFVDataStoreHandler implements DataStoreHandler {
    /**
     * The logger.
     */
    private final static Logger _logger = Logger.getLogger(FKFVDataStoreHandler.class);
    
    /**
     * The number of bytes required for fixed-length keys.
     */
    private final int _keyLength;
    
    /**
     * The number of bytes required for fixed-length values.
     */
    private final int _valLength;
    
    /**
     * Creates a new instance of FKFVDataStoreHandler.
     * 
     * @param keyLength   - the number of bytes required for fixed-length keys.
     * @param valueLength - the number of bytes required for fixed-length values.
     */
    protected FKFVDataStoreHandler(int keyLength, int valueLength) {
        this._keyLength = keyLength;
        this._valLength = valueLength;
    }
    
    /**
     * Gets the number of bytes required for fixed-length keys.
     */
    public final int getKeyLength() {
        return _keyLength;
    }
    
    /**
     * Gets the number of bytes required for fixed-length values.
     */
    public final int getValueLength() {
        return _valLength;
    }
    
    @Override
    public final byte[] assemble(byte[] key, byte[] value) {
        if(value == null) return null;
        
        byte[] result = new byte[Bytes.NUM_BYTES_IN_INT + _keyLength + _valLength];
        ByteBuffer bb = ByteBuffer.wrap(result);
        
        // count
        bb.putInt(1);
        
        // add key
        bb.put(key);
        
        // add value
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
        
        byte[] result = new byte[newLength + _keyLength + _valLength];
        System.arraycopy(data, 0, result, 0, newLength);
        
        ByteBuffer bb = ByteBuffer.wrap(result);
        
        int cnt = bb.getInt();
        
        // update count
        bb.position(0);
        bb.putInt(cnt + 1);
        
        // add key
        bb.position(newLength);
        bb.put(key);
        
        // add value
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
                if(Bytes.equals(key, data, bb.position(), _keyLength)) {
                    return originalCnt;
                }
                bb.position(bb.position() + _keyLength + _valLength);
                
                cnt--;
            }
            
            return -originalCnt;
        } catch (Exception e) {
            _logger.error("Failed to countCollisions", e);
            return 0;
        }
    }
    
    @Override
    public final byte[] extractByKey(byte[] key, byte[] data) {
        if(data == null || data.length == 0) return null;
        ByteBuffer bb = ByteBuffer.wrap(data);
        
        int cnt = bb.getInt();
        while(cnt > 0) {
            // Process key
            if(Bytes.equals(key, data, bb.position(), _keyLength)) {
                // Pass key data
                bb.position(bb.position() + _keyLength);
                
                // Process value
                byte[] result = new byte[_valLength];
                bb.get(result);
                
                return result;
            }
            bb.position(bb.position() + _keyLength + _valLength);
            
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
            if(Bytes.equals(key, data, bb.position(), _keyLength)) {
                bb.position(bb.position() + _keyLength + _valLength);
                offset2 = bb.position();
                break;
            }
            bb.position(bb.position() + _keyLength + _valLength);
            
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
    
    @Override
    public final List<byte[]> extractKeys(byte[] data) {
        try {
            ByteBuffer bb = ByteBuffer.wrap(data);
            int cnt = bb.getInt();
            final List<byte[]> result = new ArrayList<byte[]>(cnt);
            
            while(cnt > 0) {
                // Process key
                byte[] key = new byte[_keyLength];
                bb.get(key);
                
                // Add to result
                result.add(key);
                
                // Process value
                bb.position(bb.position() + _valLength);
                
                cnt--;
            }
            
            return result;
        } catch(Exception e) {
            _logger.error("Failed to extractKeys", e);
            return null;
        }
    }
    
    @Override
    public final List<byte[]> extractValues(byte[] data) {
        try {
            ByteBuffer bb = ByteBuffer.wrap(data);
            int cnt = bb.getInt();
            final List<byte[]> result = new ArrayList<byte[]>(cnt);
            
            while(cnt > 0) {
                // Process key
                bb.position(bb.position() + _keyLength);
                
                // Process value
                byte[] value = new byte[_valLength];
                bb.get(value);
                
                // Add to result
                result.add(value);
                
                cnt--;
            }
            
            return result;
        } catch (Exception e) {
            _logger.error("Failed to extractValues", e);
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
                byte[] key = new byte[_keyLength];
                bb.get(key);
                
                // Process value
                byte[] val = new byte[_valLength];
                bb.get(val);
                
                // Add to result
                result.add(new SimpleEntry<byte[], byte[]>(key, val));
                
                cnt--;
            }
            
            return result;
        } catch(Exception e) {
            _logger.error("Failed to extractEntries", e);
            return null;
        }
    }
    
    @Override
    public final byte[] assembleEntries(List<Entry<byte[], byte[]>> entries) {
        byte[] b;
        int cnt = 0;
        int len = Bytes.NUM_BYTES_IN_INT;
        
        for(Entry<byte[], byte[]> e : entries) {
            b = e.getKey();
            if(b != null) {
                len += _keyLength;
                len += _valLength;
                cnt++;
            }
        }
        
        byte[] data = new byte[len];
        ByteBuffer bb = ByteBuffer.wrap(data);
        
        bb.putInt(cnt);
        for(Entry<byte[], byte[]> e : entries) {
            b = e.getKey();
            if(b != null) {
                bb.put(b);
                
                b = e.getValue();
                bb.put(b);
            }
        }
        
        return data;
    }
}
