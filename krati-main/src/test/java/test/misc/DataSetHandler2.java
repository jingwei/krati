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

package test.misc;

import java.nio.ByteBuffer;

import krati.store.DataSetHandler;

/**
 * DataSetHandler2
 * 
 * @author jwu
 * 
 */
public final class DataSetHandler2 implements DataSetHandler {
    
    @Override
    public final byte[] assemble(byte[] value) {
        byte[] result = new byte[4 + value.length];
        ByteBuffer bb = ByteBuffer.wrap(result);
        
        bb.putInt(value.length);
        bb.put(value);
        
        return result;
    }
    
    @Override
    public final byte[] assemble(byte[] value, byte[] data) {
        byte[] result = new byte[data.length + 4 + value.length];
        System.arraycopy(data, 0, result, 0, data.length);
        ByteBuffer bb = ByteBuffer.wrap(result);
        
        bb.position(data.length);
        bb.putInt(value.length);
        bb.put(value);
        
        return result;
    }
    
    @Override
    public final boolean find(byte[] value, byte[] data) {
        try {
            ByteBuffer bb = ByteBuffer.wrap(data);
            while(bb.remaining() >= 4) {
                // Process value
                int len = bb.getInt();
                if(len <= bb.remaining()) {
                    if(bytesEqual(value, data, bb.position(), len)) {
                        return true;
                    }
                    bb.position(bb.position() + len);
                } else {
                    break;
                }
            }
            
            return false;
        } catch(Exception e) {
            return false;
        }
    }
    
    @Override
    public final int count(byte[] data) {
        try {
            int cnt = 0;
            
            ByteBuffer bb = ByteBuffer.wrap(data);
            while(bb.remaining() >= 4) {
                // Process data value
                int len = bb.getInt();
                if (len <= bb.remaining()) {
                    cnt++;
                    bb.position(bb.position() + len);
                } else {
                    break;
                }
            }
            
            return cnt;
        } catch(Exception e) {
            e.printStackTrace();
            return 0;
        }
    }
    
    @Override
    public final int countCollisions(byte[] value, byte[] data) {
        try {
            int cnt = 0;
            boolean found = false;
            
            ByteBuffer bb = ByteBuffer.wrap(data);
            while(bb.remaining() >= 4) {
                // Process data value
                int len = bb.getInt();
                if (len <= bb.remaining()) {
                    cnt++;
                    if(bytesEqual(value, data, bb.position(), len)) {
                        found = true;
                    }
                    bb.position(bb.position() + len);
                } else {
                    break;
                }
            }
            
            return found ? cnt : -cnt;
        } catch(Exception e) {
            return 0;
        }
    }
    
    @Override
    public int remove(byte[] value, byte[] data) {
        int offset1 = 0;
        int offset2 = 0;
        ByteBuffer bb = ByteBuffer.wrap(data);
        
        while(bb.remaining() >= 4) {
            offset1 = bb.position();
            
            // Process value
            int len = bb.getInt();
            if (len <= bb.remaining()) {
                if(bytesEqual(value, data, bb.position(), len)) {
                    offset2 = bb.position() + len;
                    break;
                }
                bb.position(bb.position() + len);
            }
            else {
                offset2 = offset1;
                break;
            }
        }
        
        // value is found and remove value from data
        if(offset1 < offset2) {
            int newLength = data.length - (offset2 - offset1);
            
            /*
             * entire data is removed
             */
            if(newLength == 0) return 0;
            
            /*
             * partial data is removed
             */
            
            // Shift data to the left
            for(int i = 0, len = data.length - offset2; i < len; i++) {
                data[offset1 + i] = data[offset2 + i];
            }
            
            return newLength;
        }
        
        // no data is removed
        return data.length;
    }
    
    boolean bytesEqual(byte[] bytes, byte[] bytesSource, int offset, int length) {
        if(bytes.length == length) {
            for(int i = 0; i < length; i++) {
                if(bytes[i] != bytesSource[offset + i]) return false;
            }
            return true;
        }
        
        return false;
    }
}
