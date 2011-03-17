package test.util;

import java.nio.ByteBuffer;

import krati.util.HashFunction;

public class HashFunctionInteger implements HashFunction<byte[]> {
    @Override
    public long hash(byte[] key) {
        return ByteBuffer.wrap(key).getInt();
    }
}
