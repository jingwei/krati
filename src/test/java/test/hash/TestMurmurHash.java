package test.hash;

import krati.util.HashFunction;
import krati.util.MurmurHashFunction;

public class TestMurmurHash extends EvalHash {
    public TestMurmurHash() {
        super(TestMurmurHash.class.getSimpleName());
    }

    @Override
    protected HashFunction<byte[]> createHashFunction() {
        return new MurmurHashFunction();
    }
}
