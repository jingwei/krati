package test.hash;

import krati.util.HashFunction;
import krati.util.MurmurHashFunction;

public class TestMurmurHashFunction extends EvalHashFunction {
    public TestMurmurHashFunction() {
        super(TestMurmurHashFunction.class.getSimpleName());
    }

    @Override
    protected HashFunction<byte[]> createHashFunction() {
        return new MurmurHashFunction();
    }
}
