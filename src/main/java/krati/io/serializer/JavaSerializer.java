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
 * 06/30, 2011
 * 
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
