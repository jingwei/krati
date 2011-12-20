/*
 * Copyright (c) 2010-2011 LinkedIn, Inc
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

package krati.io.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import krati.io.SerializationException;
import krati.io.Serializer;

/**
 * JavaSerializer
 * 
 * @author jwu
 * @since 06/30, 2011
 */
public class JavaSerializer<T extends Serializable> implements Serializer<T> {
    
    @Override @SuppressWarnings("unchecked")
    public T deserialize(byte[] bytes) throws SerializationException {
        if(bytes == null) {
            return null;
        }
        
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            return (T)ois.readObject();
        } catch(Exception e) {
            throw new SerializationException("Failed to deserialize bytes", e);
        }
    }
    
    @Override
    public byte[] serialize(T object) throws SerializationException {
        if(object == null) {
            return null;
        }
        
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            return baos.toByteArray();
        } catch(Exception e) {
            throw new SerializationException("Failed to serialize object", e);
        }
    }
}
