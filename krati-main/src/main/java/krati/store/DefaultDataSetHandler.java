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

package krati.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import krati.store.DataSetHandler;

/**
 * DefaultDataSetHandler
 * 
 * @author jwu
 *
 */
public final class DefaultDataSetHandler implements DataSetHandler {
    /**
     * The logger.
     */
    private final static Logger _log = Logger.getLogger(DefaultDataSetHandler.class);
    
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
    
    @Override
    public final List<byte[]> extractValues(byte[] data) {
        try {
            final List<byte[]> result = new ArrayList<byte[]>();
            ByteBuffer bb = ByteBuffer.wrap(data);
            int cnt = bb.getInt();
            
            while(cnt > 0) {
                // Read value
                int len = bb.getInt();
                if (len <= bb.remaining()) {
                    byte[] value = new byte[len];
                    bb.get(value);
                
                    result.add(value);
                } else {
                    break;
                }
                
                cnt--;
            }
            
            return result;
        } catch(Exception e) {
            _log.error("Failed to extractValues", e);
            return null;
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
