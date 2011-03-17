package test.protos;

import krati.sos.ObjectSerializer;

public class KeySerializer implements ObjectSerializer<String> {
    @Override
    public String construct(byte[] binary) {
        return new String(binary);
    }

    @Override
    public byte[] serialize(String object) {
        return object.getBytes();
    }
}
