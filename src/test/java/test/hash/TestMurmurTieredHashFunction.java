package test.hash;

import krati.util.HashFunction;
import krati.util.MurmurHashFunction;

public class TestMurmurTieredHashFunction extends EvalTieredHashFunction {
    public TestMurmurTieredHashFunction() {
        super(TestMurmurTieredHashFunction.class.getSimpleName());
    }

    @Override
    protected HashFunction<byte[]> createHashFunction() {
        return new MurmurHashFunction();
    }
}
