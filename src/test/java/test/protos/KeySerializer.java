package test.protos;

import krati.io.Serializer;

public class KeySerializer implements Serializer<String> {
    @Override
    public String deserialize(byte[] binary) {
        return new String(binary);
    }

    @Override
    public byte[] serialize(String object) {
        return object.getBytes();
    }
}
